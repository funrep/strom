# Ström

*AI-generated code and docs (Claude).*

A bolt-on for **fs2** and **ZIO Streams**. Wrap an ordinary stream expression, and its pure
operators are fused **at compile time** into allocation-free loops. Effects and everything else
run exactly as before. Built on Scala 3 staging and Kiselyov et al.'s strymonas
([References](#references)).

```scala
import strom.fs2.StromFs2

val total: Stream[IO, Long] = StromFs2.fused {
  requests
    .map(parse).filter(_.status >= 500)     // fused
    .evalMap(logIt)                         // kept: an effect
    .map(_.latencyMs).fold(0L)(_ + _)       // fused
}
```

Results are identical to the unfused code. Typical gains: ~8× on map/filter chains, ~100× on
nested/zip pipelines, ~1 byte of garbage per element instead of 100+ ([Numbers](#numbers)).

## Install

Not on Maven Central yet: `sbt publishLocal`, then

```scala
libraryDependencies += "io.github.funrep" %% "strom-fs2" % "<version>" // or strom-zio, strom-loom, strom-core
```

Scala 3.9, JDK 21+.

## `fused`

```scala
StromFs2.fused { in.map(f).filter(p).evalMap(save).map(g).take(100L) }
StromZio.fused { in.map(f).filter(p).mapZIO(save).map(g) }
StromFs2.fusedPlan(…)   // compile-time string: "fused: map → filter … / kept: evalMap"
```

Each run of pure operators becomes one compiled segment; everything else stays as written.

* **fs2:** `map`, `filter`, `collect`, `mapFilter`, `take`, `drop`, `takeWhile`, `dropWhile`,
  `zipWithIndex`, `scan`, `mapAccumulate`, `fold`, `foldMonoid`, `zip`, `zipWith`, and `flatMap`
  onto `Stream.range` / `emit` / `emits` / `chunk` / `iterable`.
* **ZIO:** `map`, `filter`, `collect`, `take`, `drop`, `takeWhile`, `dropWhile`, `zipWithIndex`,
  `scan`, `mapAccum`, `zipWith`, and `flatMap` onto `ZStream.range` / `succeed` /
  `fromIterable` / `fromChunk`.
* Those pure sources can also start a run: `Stream.emits(xs).map(f).filter(p)` is one loop.
* Fused code yields to the scheduler (`cede` / `yieldNow`) every 100 µs, so it never starves
  other fibers.

`examples/` is a standalone project with realistic fs2 / ZIO pipelines.

## `fusedPar`: several cores

```scala
StromFs2.fusedPar(8) {
  in.map(parse).filter(valid).map(score)   // 8 workers, order kept
    .take(1000L)                           // stateful: one instance
    .fold(0L)(_ + _)
}
StromZio.fusedPar(8) { … }
StromFs2.fusedParPlan(…)  // "… parallel: replicated[map → filter → map] | single[take → fold]"
```

Same output as the sequential stream, in the same order.

* **Stateless operators** (`map`, `filter`, `collect`, `flatMap`) process chunks on N workers.
* **Stateful operators** (`take`, `scan`, `zip`, …) run on one instance; work before and
  after them still runs in parallel.
* **Folds** run as parallel partial folds when the operator is known to be associative:
  `+`, `*`, `&`, `|`, `^`, `max`, `min` on Ints/Longs starting from the identity, or fs2
  `foldMonoid`. Other folds run on one instance.
* Uses `parEvalMap` / `mapZIOPar`, one task per chunk. fs2 needs `Concurrent[F]`.

**Lambdas must be pure:** they run on several threads at once. Capturing a `var` is a compile
error; other shared mutable state isn't detected. Worth it only when each element does real work.

## Lower-level API: the `Strom` DSL

`fused` compiles through a small DSL, usable directly for arrays or hand-driven segments:

```scala
val r: Int = Strom.run {   // one loop, no allocation at all
  Strom.fromArray(a).flatMap(x => Strom.range(0, x)).zipWith(Strom.fromArray(b))(_ * _).take(n).sum
}
val seg = Strom.segment((a: Strom[Int]) => a.map(f).filter(p))  // resumable, 1 to 4 input ports
LoomParallel.plan((a: Strom[Int]) => …)   // parallel plan on plain (virtual) threads
Strom.showCode(…) / Strom.explain(…)      // generated code / IR shape, at compile time
```

## Guarantees

* **No per-element allocation** on the fused path: lambdas are inlined, element types are
  primitive, there are no intermediate chunks. Measured by `AllocationSuite`.
* **Segments are synchronous, effect-free and thread-confined**, with bounded work per step
  (`NeedInput` / `Emit` / `Yield` / `Done`). A failing step poisons the instance.
* **`strom-core` has no dependencies.** fs2, ZIO and loom live in adapter modules.

## How it works

* **Frontend:** a macro reads the source of the wrapped expression into a pipeline IR, with
  lambdas substituted in place.
* **Kiselyov IR:** producers (`For` / `Unfold`) in linear or nested streams; `take` is pushed
  into every loop, and zip picks the cheapest strategy, following strymonas.
* **Two backends:** nested `while` loops (direct mode), and a flag-per-level form whose state
  lives in fields, so a segment can stop at any element and resume.
* **Method splitting:** generated methods stay under HotSpot's 8000-byte JIT limit.
* **Runtime:** a sans-I/O runner drives segments; adapters feed it chunks.

## Verification

All pipelines are checked against a naive reference interpreter (or the plain unfused fs2/ZIO
code) for random inputs, **random chunkings** and random work budgets down to 1. Also:
random pipeline shapes compiled at test time, golden files of generated code, bytecode
method-size checks and allocation measurements. `sbt test` runs everything.

## Numbers

JMH, Apple M1 (4 performance + 4 efficiency cores), JDK 23. **Lower is better.**

**Throughput** (ns per element / bytes of garbage per element):

| | 7 cheap stages + sum | flatMap + zip + filter + take + sum |
|---|---|---|
| hand-written `while` loop | 4.6 / 0 | — |
| `Strom.run` | 4.7 / 0 | 3.3 / 0 |
| **fs2 code in `StromFs2.fused`** | **6.2 / 1.4** | **8.3 / 36** |
| plain fs2 | 52 / 108 | 825 / 3269 |
| plain ZStream | 31 / 147 | 641 / 1519 |

Fused code is within ~1.5 ns of a hand-written loop. The gain applies to time spent in stream
machinery: big for CPU-heavy pipelines, small for I/O-bound ones.

**Scaling** (ns per element, ~20 ops per element):

| workers | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| loom | 8.0 | 4.1 | 2.2 | 1.9 |
| fs2 `fusedPar` | 8.3 | 4.7 | 2.9 | 2.8 |

Near-linear up to 4 workers; the M1's other 4 cores are slower efficiency cores. ZIO and
partial-fold plans are not benchmarked yet.

**Responsiveness** (lateness of a 1 ms timer while every core runs fused work):

| | p50 | p99 |
|---|---|---|
| idle machine | 0.52 ms | 0.63 ms |
| Ström default (yields every 100 µs) | 0.13 ms | 2.9 ms |
| Ström never yielding | 1.4 s | 2.8 s |

Benchmarks: `sbt "bench/Jmh/run strom.bench.CheapStages strom.bench.Representative strom.bench.Scaling"`
and `sbt "bench/runMain strom.bench.Latency"` (run on an idle machine).

## Limits

* **By design:** no effects inside fused code; no fan-out (each input is consumed once); only
  code the macro can see is fused (write pipelines inline; functions passed as values are
  called, not inlined). Unfused operators are simply kept as written.
* At most 4 inputs per fused run, and a fixed pool of state slots per segment (a compile error
  if exceeded).
* A tuple that leaves the fused code (emitted, folded) is allocated; tuples used only inside
  are not.
* A `List` inside `flatMap` costs one iterator per element; ZIO `Chunk` reads may box.
* `fusedPar` decides where to split with a fixed rule, not a cost model.

## Relation to strymonas

Ström follows strymonas [1, 2]: zipping with the complex side driving, elements as compile-time
values (no runtime tuples), and `map_accum_filter` as the general stateful operator. It adds
resumable chunk-driven segments, fs2 / ZIO / loom adapters, parallel plans and fusion of plain
fs2 / ZIO code. Correctness rests on property tests, not mechanised proofs.

## References

1. Kiselyov, Biboudis, Palladinos, Smaragdakis. *Stream Fusion, to Completeness*. POPL 2017.
   [doi:10.1145/3009837](https://doi.org/10.1145/3009837)
2. Kiselyov, Kobayashi, Palladinos. *Complete Fusion for Stateful Streams: Equational Theory of
   Stateful Streams and Fusion as Normalization-by-Evaluation*. TOPLAS.
   [arXiv:2412.15768](https://arxiv.org/abs/2412.15768),
   [strymonas-ocaml](https://github.com/strymonas/strymonas-ocaml)
3. Kobayashi, Kiselyov. *Complete Stream Fusion for Software-Defined Radio*. PEPM 2024.
   [doi:10.1145/3635800.3636962](https://doi.org/10.1145/3635800.3636962)

## License

MIT, see [LICENSE](LICENSE).
