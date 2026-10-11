# Ström

*AI-generated code and docs (Claude).*

A bolt-on for **fs2** and **ZIO Streams**. Wrap an ordinary stream expression, and its pure
operators are fused **at compile time** into allocation-free loops. Effects and everything else
run exactly as before. Built on Scala 3 staging and Kiselyov et al.'s strymonas
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
import strom.fs2.*   // or strom.zio.*
in.map(f).filter(p).evalMap(save).map(g).take(100L).fused
in.map(f).filter(p).mapZIO(save).map(g).fused     // ZIO
in.map(f).filter(p).evalMap(save).fusedPlan       // compile-time string: "fused: map → filter / kept: evalMap"
StromFs2.fused { … }                              // the same, as a wrapper
```

The result has exactly the type you passed in (`Stream[IO, A]`, `ZStream[R, E, A]`); no Ström
types appear in your code. Each run of pure operators becomes one compiled segment; everything
else stays as written.

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
in.map(parse).filter(valid).map(score)   // 8 workers, order kept
  .take(1000L)                           // stateful: one instance
  .fold(0L)(_ + _)
  .fusedPar(8)
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

Besides the operators `fused` maps onto, the DSL has `windowFold(size)(z)(op)` (tumbling
windows, each folded) and `foldAssociative(z)(op)`, a fold you declare associative so that
parallel plans can run it as partial folds.

## Guarantees

* **No per-element allocation** on the fused path: lambdas are inlined, element types are
  primitive, there are no intermediate chunks. Measured by `AllocationSuite`.
* **Segments are synchronous, effect-free and thread-confined**, with bounded work per step
  (`NeedInput` / `Emit` / `Yield` / `Done`). A failing step poisons the instance.
* **`strom-core` has no dependencies.** fs2, ZIO and loom live in adapter modules.

## How it works

**What fusion is.** In plain fs2 or ZIO, every operator is a separate stage. Each element goes
through a chain of objects: a closure call per operator, a new intermediate chunk per stage,
and boxing (an `Int` becomes an `Integer`) because the stages are generic. Fusion compiles the
whole chain into one loop instead, with your lambdas pasted in. For example

```scala
in.map(_ * 3).filter(_ % 2 == 0).take(100L).fold(0)(_ + _)
```

becomes, per input chunk, roughly

```scala
while (i < chunk.length && taken < 100) {
  val x = chunk(i) * 3
  if (x % 2 == 0) { acc += x; taken += 1 }
  i += 1
}
```

No closures, no intermediate chunks, no boxing: the JIT sees plain arithmetic on primitives.

**The steps:**

1. **Read the code.** `.fused` is a macro: at compile time it sees your expression's source,
   splits it into runs of pure operators, and leaves everything else (effects, time,
   concurrency) as written. That is why only code written inline can be fused.
2. **Build loops.** Each run is translated into Kiselyov et al.'s stream IR: a source is
   either indexed (an array, a range: a `for` loop) or stepped (an iterator, an input port: a
   `while` loop). `flatMap` becomes a nested loop, `filter` an `if`, and `take` adds a counter
   checked in every loop. `zip` is the hard case, because two loops must advance in step; the
   generator picks the cheapest strategy (shared index, lock-step, or turning only the simpler
   side into a pull), following strymonas.
3. **Make it resumable.** fs2 and ZIO deliver input in chunks, and the consumer may want
   output before the input ends. So the loop keeps its state (counters, positions,
   accumulators) in fields of a small object, a *segment*, and can stop at any element and
   continue later. Each `step()` runs until it needs the next input chunk, has a batch of
   output, has used its work budget, or is done.
4. **Drive it.** A small runner translates those four results into fs2 `Pull`s or ZIO
   channel steps: pull the next chunk, emit a chunk, give way to other fibers (every ~100 µs),
   or finish.
5. **Stay JIT-friendly.** HotSpot doesn't compile methods over 8000 bytes of bytecode, so very
   large pipelines are split into several methods.

`fusedPar` adds one step: it looks at which operators carry state and cuts the run there.
Stateless parts become copies of the segment working on different chunks at once, with the
outputs put back in order.

## Verification

Every check compares Ström's output with a trusted answer. There are two kinds, depending on
the layer:

* **The compiler (Strom DSL, segments, parallel plans)** is checked against a *reference
  interpreter*: a ~30-line implementation of every operator on top of Scala iterators
  (`Reference.scala`, in the test sources). It is written to be obviously correct, not fast. Each pipeline runs on
  random inputs, cut into **random chunks** (including empty ones), with random output buffer
  sizes and random work budgets down to 1, so every place a segment can stop and resume is
  exercised. A further suite generates **random pipeline shapes**, compiles them at test time
  with the same code generator, and checks those too.
* **The fs2 / ZIO integration (`.fused`, `.fusedPar`)** is checked against the **same plain
  fs2 / ZIO code without Ström**, which is the behaviour users already rely on:
  * A **fuzzer** (`FuzzSuite`, generated by `project/FuzzGen.scala`) writes random operator
    chains (pure operators mixed with `evalMap` / `mapZIO`, `evalFilter`, `rechunk`, zips with
    effectful streams, folds, and lambdas that throw) and runs each in three versions, plain,
    `.fused` and `.fusedPar(3)`, on random inputs and chunkings. Output and the **log of every
    effect, in order,** must match plain code exactly. 120 chains per library run by default;
    `STROM_FUZZ_SEED` / `STROM_FUZZ_CASES` run more (about 1,900 chains have passed).
  * `RealWorldSuite` checks the rest: `bracket` / `Resource` / `acquireRelease` finalizers (also
    on early `take` and on failure), interruption and timeouts (including endless loops that
    never emit), infinite sources with `take`, `parJoin` / `parEvalMap` / `merge` /
    `concurrently` / `flatMapPar`, one fused stream run 64 times in parallel, `metered` /
    `groupWithin`, and pull-based consumers.

  **When a lambda throws**, fused code emits every element computed before the failing one,
  then fails with the same error. Plain fs2 / ZIO apply an operator to a whole chunk at once,
  so they may drop more elements of that chunk, or fail on an element that a later `take`
  never needs, where fused code succeeds. Fused code never fails where plain code succeeds.
  `.fusedPar` works ahead of demand, but holds every error until its turn in order. With ZIO,
  `zipWith` pulls both sides concurrently, so effects near the end of a zip may run one pull
  apart; everywhere else effects match exactly.

Also: golden files of the generated code, bytecode checks that no generated method exceeds the
JIT limit, and allocation measurements (no per-element garbage). `sbt test` runs everything.

## Numbers

JMH, Apple M1 (4 performance + 4 efficiency cores), JDK 23. **Lower is better.**

**Throughput** (ns per element / bytes of garbage per element). Every row computes the same
pipeline on the same data. Pipeline A: `map`/`filter` ×7 then sum. Pipeline B:
`a.flatMap(x => range(0, x)).zipWith(b.filter(even))(_ * _).take(n).sum`.

| Row | Fused? | A | B |
|---|---|---|---|
| hand-written `while` loop (no library: the floor) | — | 4.6 / 0 | 3.0 / 0 |
| `Strom.run` over arrays (Ström DSL) | yes | 4.7 / 0 | 3.2 / 0 |
| plain fs2 code (the baseline) | no | 51 / 108 | 804 / 3269 |
| **same fs2 code + `.fused`** | yes | **5.9 / 1.4** | **8.8 / 36** |
| same fs2 code + `.fusedPar(4)` | yes, 4 cores | 2.9 / 5.6 | 18.7 / 80 |
| plain ZStream code (the baseline) | no | 31 / 147 | 632 / 1525 |
| **same ZStream code + `.fused`** | yes | **9.5 / 20** | **18.8 / 70** |
| same ZStream code + `.fusedPar(4)` | yes, 4 cores | 4.0 / 25 | 16.3 / 136 |

* Fused fs2 code is within ~1.5 ns (A) and ~6 ns (B) of a hand-written loop: ~8× and ~100×
  faster than plain fs2. The remaining gap is fs2 itself (chunk hand-off, the effect loop) and,
  in B, pulling the two zipped streams in exactly fs2's order (left first), so that their
  effects run as in plain code.
* Fused ZStream code is ~3× and ~35× faster than plain ZStream. It's slower than fs2 here
  because the benchmark ends in `runSum`, a ZIO sink that isn't fused (it boxes each element).
* `fusedPar(4)` doubles the speed of the simple pipeline. On the zip pipeline it loses for
  fs2: the zip with another stream splits the work into three runs, and the parallel part (a
  cheap `flatMap`) doesn't pay for the extra hand-offs.
* The gain applies to time spent in stream machinery: big for CPU-heavy pipelines, small for
  I/O-bound ones.

**Scaling** (ns per element, ~20 ops per element; one thread without any executor: 7.7):

| workers | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| loom | 8.8 | 4.5 | 2.5 | 2.0 |
| fs2 `fusedPar` | 8.6 | 4.8 | 2.9 | 2.9 |
| fs2 `fusedPar` + fold (partial folds) | 7.6 | 3.8 | 2.7 | 2.3 |
| ZIO `fusedPar` | 8.6 | 4.7 | 3.0 | 3.1 |

Near-linear up to 4 workers (~3×); the M1's other 4 cores are slower efficiency cores, so 8
adds little. Partial folds scale best of the stream executors: only one small result per chunk
goes back through the stream.

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
* **Inputs:** a fused run reads at most 4 streams: your stream plus each *effectful* stream
  zipped into it (a pure operand like `Stream.range(…).map(f)` is built into the loop and
  doesn't count). With `fused`, extra zips are simply left unfused; `Strom.segment` takes at most
  4 parameters.
* **State:** operator state (`take` counters, `scan` accumulators, `flatMap` positions, zip
  bookkeeping) lives in a fixed pool of slots per segment, enough for roughly a hundred
  operators in one chain. Running out is a compile error, never a runtime failure.
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
   [doi:10.1145/3009837.3009880](https://doi.org/10.1145/3009837.3009880)
2. Kiselyov, Kobayashi, Palladinos. *Complete Fusion for Stateful Streams: Equational Theory of
   Stateful Streams and Fusion as Normalization-by-Evaluation*. Preprint, 2024.
   [arXiv:2412.15768](https://arxiv.org/abs/2412.15768),
   [strymonas-ocaml](https://github.com/strymonas/strymonas-ocaml)
3. Kobayashi, Kiselyov. *Complete Stream Fusion for Software-Defined Radio*. PEPM 2024.
   [doi:10.1145/3635800.3636962](https://doi.org/10.1145/3635800.3636962)

## License

MIT, see [LICENSE](LICENSE). The stream IR follows strymonas (also MIT); its copyright notice
is in [NOTICE](NOTICE).
