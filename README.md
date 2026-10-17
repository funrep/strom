# Ström

*AI-generated code and docs (Claude).*

Compile-time fusion for Scala 3. Wrap ordinary **fs2** / **ZIO Streams** code, and its pure
operators become one allocation-free loop. Wrap ordinary **cats-effect `IO`** / **ZIO** code, and
its synchronous steps become plain Scala. Effects and everything else run exactly as before, with
the same results. Built on Scala 3 staging and Kiselyov et al.'s strymonas
([References](#references)).

```scala
import strom.fs2.*

val total: Stream[IO, Long] =
  requests
    .map(parse).filter(_.status >= 500)     // fused
    .evalMap(logIt)                         // kept: an effect
    .map(_.latencyMs).fold(0L)(_ + _)       // fused
    .fused
```

Typical gains: ~8× on map/filter chains, ~100× on nested/zip pipelines, 2–60× on synchronous
effect code ([Numbers](#numbers)).

## Install

Not on Maven Central yet: `sbt publishLocal`, then

```scala
libraryDependencies += "io.github.funrep" %% "strom-fs2" % "<version>"
// or strom-zio, strom-loom, strom-core (streams), strom-effect (IO / ZIO code)
```

Scala 3.9, JDK 21+.

## Streams: `fused`

```scala
import strom.fs2.*   // or strom.zio.*
in.map(f).filter(p).evalMap(save).map(g).take(100L).fused
in.map(f).filter(p).mapZIO(save).map(g).fused     // ZIO
in.map(f).filter(p).evalMap(save).fusedPlan       // compile-time string: "fused: map → filter / kept: evalMap"
StromFs2.fused { … }                              // the same, as a wrapper
```

The result has exactly the type you passed in; no Ström types appear in your code. Each run of
pure operators becomes one compiled segment; everything else stays as written.

* **fs2:** `map`, `filter`, `collect`, `mapFilter`, `take`, `drop`, `takeWhile`, `dropWhile`,
  `zipWithIndex`, `scan`, `mapAccumulate`, `fold`, `foldMonoid`, `zip`, `zipWith`, and `flatMap`
  onto `Stream.range` / `emit` / `emits` / `chunk` / `iterable`.
* **ZIO:** `map`, `filter`, `collect`, `take`, `drop`, `takeWhile`, `dropWhile`, `zipWithIndex`,
  `scan`, `mapAccum`, `zipWith`, and `flatMap` onto `ZStream.range` / `succeed` /
  `fromIterable` / `fromChunk`.
* Those pure sources can also start a run: `Stream.emits(xs).map(f).filter(p)` is one loop.
* Fused code yields (`cede` / `yieldNow`) every 100 µs, so it never starves other fibers.

`examples/` is a standalone project with realistic fs2 / ZIO pipelines.

### `fusedPar`: several cores

```scala
in.map(parse).filter(valid).map(score)   // 8 workers, order kept
  .take(1000L)                           // stateful: one instance
  .fold(0L)(_ + _)
  .fusedPar(8)
```

Same output as the sequential stream, in the same order. Stateless operators (`map`, `filter`,
`collect`, `flatMap`) run on N workers; stateful ones (`take`, `scan`, `zip`, …) on one instance.
Folds with a known associative operator (`+`, `*`, `&`, `|`, `^`, `max`, `min` on Ints/Longs from
the identity, or fs2 `foldMonoid`) run as parallel partial folds. fs2 needs `Concurrent[F]`.

**Lambdas must be pure:** they run on several threads at once. Capturing a `var` is a compile
error; other shared mutable state isn't detected. Worth it only when each element does real work.

### The `Strom` DSL

`fused` compiles through a small DSL, usable directly:

```scala
val r: Int = Strom.run {   // one loop, no allocation at all
  Strom.fromArray(a).flatMap(x => Strom.range(0, x)).zipWith(Strom.fromArray(b))(_ * _).take(n).sum
}
val seg = Strom.segment((a: Strom[Int]) => a.map(f).filter(p))  // resumable, 1 to 4 input ports
LoomParallel.plan((a: Strom[Int]) => …)   // parallel plan on plain (virtual) threads
Strom.showCode(…) / Strom.explain(…)      // generated code / IR shape, at compile time
```

It also has `windowFold(size)(z)(op)` (tumbling windows) and `foldAssociative(z)(op)` (a fold you
declare associative, so parallel plans can split it).

## Effect code: `StromEffect.fused`

```scala
import strom.effect.*

def checkout(order: Order): IO[Receipt] = StromEffect.fused {
  for
    _     <- if order.items.isEmpty then IO.raiseError(Rejected("empty")) else IO.unit
    user  <- users.find(order.user)                 // a service call: stays a call
    total <- priceAll(order.items, 0L)              // a local def: compiled too
    disc   = if user.vip then total / 10 else 0L
    _     <- sales.record(total - disc)
  yield Receipt(order.id, total - disc)
}
```

* **Put it on each method you want faster** (a handler, a loop, a validation step). Only code
  written inside the block is compiled, including local `def`s; calls to other methods stay
  calls. A block with nothing to gain is left exactly as written, so wrapping too much is
  harmless.
* **Fused:** cats-effect `pure`, `delay`, `unit`, `raiseError`, `map`, `flatMap`, `*>`, `>>`,
  `as`, `void`, `handleErrorWith`, `attempt`; ZIO `succeed`, `fail`, `die`, `attempt`, `map`,
  `flatMap`, `*>`, `as`, `catchAll`, `either`; `if` / `match`; local `def`s, which become
  `while` loops when they recurse. Everything else (`Ref`, `sleep`, `IO.both`, masks, your
  methods) is kept as a call. Code written against `F[_]: Sync` is not fused.
* Same values, same errors, same order of effects, same cancelation. A fused loop yields every
  1024 iterations, as cats-effect does.
* `StromEffect.show { … }` prints the generated code. `-Dstrom.effect.fuse=off` turns fusing off
  everywhere (on cats-effect, fused steps don't appear in async traces).

## How it works

In plain fs2 or ZIO, every operator is a separate stage: a closure call per operator, an
intermediate chunk per stage, and boxing. `fused` is a macro: at compile time it reads your
expression, splits it into runs of pure operators, and compiles each run into one loop with your
lambdas inlined. For example `in.map(_ * 3).filter(_ % 2 == 0).take(100L).fold(0)(_ + _)` becomes,
per input chunk, roughly

```scala
while (i < chunk.length && taken < 100) {
  val x = chunk(i) * 3
  if (x % 2 == 0) { acc += x; taken += 1 }
  i += 1
}
```

The loop follows strymonas' stream IR (including its strategies for `zip`). Its state lives in a
small resumable object, a *segment*, so it can stop at any element when it needs input, has
output, or has used its time slice; a runner drives it from fs2 `Pull`s or ZIO channels. Large
pipelines are split into several methods to stay under HotSpot's 8000-byte JIT limit. `fusedPar`
cuts the run at stateful operators and runs copies of the stateless parts on several chunks at
once.

`StromEffect.fused` does the same for effect code: synchronous steps become direct code, and the
code continues in a `flatMap` only at a real effect.

## Verification

Every check compares against a trusted answer; `sbt test` runs them all.

* **Stream compiler:** checked against a ~30-line reference interpreter over Scala iterators, on
  random inputs cut into random chunks with random buffer sizes and work budgets down to 1, so
  every place a segment can stop and resume is exercised; also on randomly generated pipeline
  shapes.
* **fs2 / ZIO integration:** a fuzzer (`project/FuzzGen.scala`) runs random operator chains (with
  effects, zips, folds and throwing lambdas) plain, `.fused` and `.fusedPar(3)`: output and the
  log of every effect, in order, must match plain code. `RealWorldSuite` covers finalizers,
  interruption, timeouts, infinite sources, concurrency operators and pull-based consumers.
* **Effect code:** a fuzzer (`project/EffectFuzzGen.scala`) runs random IO / ZIO programs (loops,
  errors, handlers, `Ref`s) plain and fused: same result or error and the same effect log. Suites
  cover cancelation, fairness, virtual time, traces and the order in which effects are built.
* Also: golden files of generated code, bytecode size checks, allocation measurements.
  `STROM_FUZZ_SEED` / `STROM_FUZZ_CASES` run more fuzz cases.

**When a stream lambda throws**, fused code emits every element computed before the failing one,
then fails with the same error. Plain fs2 / ZIO work a chunk at a time, so they may drop more of
that chunk, or fail on an element a later `take` never needs. Fused code never fails where plain
code succeeds. With ZIO, `zipWith` pulls both sides concurrently, so effects near the end of a zip
may run one pull apart; everywhere else effects match exactly.

## Numbers

JMH, Apple M1 (4 performance + 4 efficiency cores), JDK 23. **Lower is better.**

**Streams** (ns / bytes of garbage per element). Pipeline A: `map`/`filter` ×7 then sum.
Pipeline B: `a.flatMap(x => range(0, x)).zipWith(b.filter(even))(_ * _).take(n).sum`.

| Row | A | B |
|---|---|---|
| hand-written `while` loop (the floor) | 4.6 / 0 | 3.0 / 0 |
| `Strom.run` over arrays | 4.6 / 0 | 3.3 / 0 |
| plain fs2 | 52 / 108 | 860 / 3269 |
| **fs2 + `.fused`** | **5.8 / 1.4** | **8.8 / 36** |
| fs2 + `.fusedPar(4)` | 3.2 / 6.8 | 18.7 / 80 |
| plain ZStream | 31 / 147 | 633 / 1523 |
| **ZStream + `.fused`** | **9.3 / 20** | **18.8 / 70** |
| ZStream + `.fusedPar(4)` | 4.0 / 25 | 16.3 / 136 |

Fused ZStream is slower than fused fs2 here because the benchmark ends in `runSum`, a ZIO sink
that isn't fused. On B, `fusedPar` loses for fs2: the zip splits the work into three runs and the
parallel part is too cheap to pay for the hand-offs. Gains apply to time spent in stream
machinery: big for CPU-heavy pipelines, small for I/O-bound ones.

**Scaling** (ns per element, ~20 ops per element; one thread, no executor: 7.7):

| workers | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| loom | 8.7 | 4.5 | 2.7 | 2.1 |
| fs2 `fusedPar` | 8.7 | 4.9 | 3.0 | 2.8 |
| fs2 `fusedPar` + fold (partial folds) | 7.6 | 3.9 | 2.7 | 2.3 |
| ZIO `fusedPar` | 8.5 | 4.6 | 3.1 | 3.1 |

Near-linear up to 4 workers; the M1's other 4 cores are slower efficiency cores.

**Responsiveness** (lateness of a 1 ms timer while every core runs fused work):

| | p50 | p99 |
|---|---|---|
| idle machine | 0.52 ms | 0.63 ms |
| Ström default (yields every 100 µs) | 0.13 ms | 2.9 ms |
| Ström never yielding | 1.4 s | 2.8 s |

**Effect code** (ns per run, plain → fused):

| Program | cats-effect | ZIO |
|---|---|---|
| cats-effect's `HandleError` benchmark | 57–72 → 1.2 (**48–60×**) | |
| cats-effect's `DeepBind` / `ShallowBind` (pure, delay) | 11–17 → 1.2 (**8–13×**) | |
| cats-effect's `DeepBind` / `ShallowBind` (with `cede`) | 46–50 → 25 (**2×**) | |
| cats-effect's `Attempt` benchmark | 21–46 → 15 (**1.5–3×**) | |
| six synchronous steps in a loop | 112 → 2.2 (**51×**) | 115 → 2.5 (**46×**) |
| a step that fails 1 in 10, recovered | 461 → 35 (**13×**) | 47 → 3.1 (**15×**) |
| sync steps + one `Ref.update` per iteration | 55 → 21 (**2.6×**) | 63 → 29 (**2.2×**) |
| order checkout handler (service calls, a `Ref`, validation) | 446 → 420 (**6%**) | 376 → 348 (**8%**) |
| only service calls (`Ref.modify` loop, `IO.both`) | unchanged | unchanged |

The first four rows are cats-effect's own run-loop benchmarks (v3.7.1), copied as written. With
real I/O, handler gains shrink in proportion.

Run them with `sbt "bench/Jmh/run strom.bench.CheapStages strom.bench.Representative strom.bench.Scaling"`,
`sbt "bench/runMain strom.bench.Latency"` and `sbt "bench/Jmh/run -prof gc strom.bench.effect"`,
on an idle machine.

## Limits

* Only code the macro can see is fused: write it inline. Functions passed as values are called,
  not inlined; methods called from a fused block are not fused.
* Streams: no effects inside fused code; no fan-out; a fused run reads at most 4 streams (extra
  zips are left unfused); operator state has a fixed pool of slots (roughly a hundred operators
  per chain; running out is a compile error). A tuple that leaves fused code is allocated. A
  `List` inside `flatMap` costs one iterator per element; ZIO `Chunk` reads may box.
* `fusedPar` decides where to split with a fixed rule, not a cost model.
* `strom-effect` depends on both cats-effect and ZIO.

## References

1. Kiselyov, Biboudis, Palladinos, Smaragdakis. *Stream Fusion, to Completeness*. POPL 2017.
   [doi:10.1145/3009837.3009880](https://doi.org/10.1145/3009837.3009880)
2. Kiselyov, Kobayashi, Palladinos. *Complete Fusion for Stateful Streams: Equational Theory of
   Stateful Streams and Fusion as Normalization-by-Evaluation*. Preprint, 2024.
   [arXiv:2412.15768](https://arxiv.org/abs/2412.15768),
   [strymonas-ocaml](https://github.com/strymonas/strymonas-ocaml)
3. Kobayashi, Kiselyov. *Complete Stream Fusion for Software-Defined Radio*. PEPM 2024.
   [doi:10.1145/3635800.3636962](https://doi.org/10.1145/3635800.3636962)
4. [cats-effect benchmarks](https://github.com/typelevel/cats-effect/tree/v3.7.1/benchmarks/src/main/scala/cats/effect/benchmarks)
   (v3.7.1, Apache 2.0), copied in `bench/src/main/scala/strom/bench/effect/CEBench.scala`.

## License

MIT, see [LICENSE](LICENSE). The stream IR follows strymonas (also MIT); its copyright notice is
in [NOTICE](NOTICE).
