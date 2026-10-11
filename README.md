# Ström

*AI-generated code (Claude).*

A bolt-on for **fs2** and **ZIO Streams** codebases: wrap an ordinary stream expression and its
pure operators are fused, **at compile time**, into allocation-free loops, while effects and
everything else run exactly as before. Built on staging (Scala 3 quotes) and Kiselyov et al.'s
strymonas ([References](#references)).

```scala
import strom.fs2.StromFs2

val total: Stream[IO, Long] = StromFs2.fused {
  requests
    .map(parse).filter(_.status >= 500)     // fused
    .evalMap(logIt)                         // kept: an effect
    .map(_.latencyMs).fold(0L)(_ + _)       // fused
}

StromFs2.fusedPlan(...)  // compile-time report: "fused: map → filter … / kept: evalMap"
```

Same for ZIO with `StromZio.fused { … }`. Swap in `fusedPar(n) { … }` to also spread the fused
work over `n` cores ([Running on several cores](#running-on-several-cores)). No rewrite into a new
DSL; results are identical to the unfused code. Typical gains: ~8× on simple map/filter chains, ~100× on nested/zip pipelines, with
~1 byte of garbage per element instead of 100+ (see [Numbers](#numbers)).

## Install

Not on Maven Central yet. Build it locally:

```
sbt publishLocal
```

```scala
libraryDependencies += "io.github.funrep" %% "strom-fs2" % "<version printed by publishLocal>" // or strom-zio, strom-loom, strom-core
```

Scala 3.9, JDK 21+. CI runs `sbt test` on every push (`.github/workflows/ci.yml`); pushing a tag
`vX.Y.Z` publishes to Maven Central (`release.yml`, once the Sonatype/PGP secrets are configured).

## Fusing plain fs2 / ZIO code

You don't have to rewrite anything in the `Strom` DSL. Wrap an ordinary stream expression:

```scala
StromFs2.fused {
  in.map(_ * 3).filter(_ % 2 == 0).evalMap(save).map(_ + 1).take(100L).fold(0)(_ + _)
}
StromZio.fused { in.map(f).filter(p).mapZIO(save).map(g) }
```

Each maximal run of pure operators becomes one compiled segment. Everything else (effects,
time, concurrency, operators it doesn't know) is kept exactly as written, between the runs.
`StromFs2.fusedPlan(…)` / `StromZio.fusedPlan(…)` return what was fused and which operators were
kept, as a compile-time string. Calls through cats syntax (`mapFilter`), extension methods and
default arguments (`takeWhile(p)`) are recognised. `examples/` is a standalone project that uses
the published artifacts on realistic fs2 / ZIO pipelines.

| | fs2 | ZIO |
|---|---|---|
| fused operators | `map`, `filter`, `collect`, `mapFilter` (cats syntax), `take`, `drop`, `takeWhile`, `dropWhile`, `zipWithIndex`, `scan`, `mapAccumulate`, `fold`, `zip`, `zipWith`, `foldMonoid`, `flatMap` onto `Stream.range` / `emit` / `emits` / `chunk` / `iterable` | `map`, `filter`, `collect`, `take`, `drop`, `takeWhile`, `dropWhile`, `zipWithIndex`, `scan`, `mapAccum`, `zipWith`, `flatMap` onto `ZStream.range` / `succeed` / `fromIterable` / `fromChunk` |
| inputs per run | up to 4 (`zipWith`; a pure `zipWith` operand is inlined instead) | up to 4 |
| yields to the scheduler | `cede` when a `Spawn[F]` exists (not for `Pure`) | `yieldNow` |

A run must be worth a segment boundary: two or more operators, or a `flatMap`, `zipWith` or `fold`.
These pure sources also start a run (`Stream.emits(xs).map(f).filter(p)` is one loop). A `List`
or other non-indexed collection is read through one iterator per `flatMap` element; chunks and
`IndexedSeq`s are read by index.
The library-independent part lives in `strom.internal.ChainFuser`; each adapter only says
which calls are pure operators and how to run a segment.

## Running on several cores

```scala
StromFs2.fusedPar(parallelism = 8) {
  in.map(parse).filter(valid).map(score)   // chunks processed by 8 workers, order kept
    .take(1000L)                           // one instance: stateful
    .fold(0L)(_ + _)                       // associative: partial folds combined in order
}
StromZio.fusedPar(8) { … }
StromFs2.fusedParPlan(…)  // "fused: map → filter → map → take → fold (1 inputs),
                          //  parallel: replicated[map → filter → map] | single[take → fold]"
```

`fusedPar` is `fused`, plus a compile-time plan for each fused run. The output is exactly that
of the sequential stream, in the same order.

* **Stateless operators** (`map`, `filter`, `collect`, `mapFilter`, `flatMap`): chunks go to
  N workers, and results are put back in input order.
* **Stateful operators** (`take`, `drop`, `scan`, `zip`, `zipWithIndex`, `mapAccum…`,
  `takeWhile`, …): one instance. The run is cut there, so stateless work before and after it
  still runs in parallel (light trailing work stays fused with the stateful operator).
* **`fold`** with a recognised associative operator and its identity as the start value
  (`_ + _` from 0, `_ * _` from 1, `&`, `|`, `^`, `math.max` / `math.min`, on Ints or Longs), or
  fs2 `foldMonoid` (associative by the cats Monoid laws): each chunk is folded separately and
  the partial results are combined in order. Any other fold runs on one instance (Double `+` is
  left out on purpose: regrouping changes the rounding).
* **`zip` / `zipWith` with another effectful stream**: the zip itself runs on one instance;
  the work before and after it gets its own parallel runs (and the other stream's own pure
  operators are fused and parallelised separately).
* Effects and other unfused operators stay where they are, as with `fused`. Runs with no
  parallel part run sequentially. fs2 needs `Concurrent[F]`; without it the run stays sequential.

**Safety.** The lambdas of fused operators run on several threads at once, so they must be pure.
A lambda that reads or writes a captured `var` is rejected at compile time. Other shared mutable
state (a mutable map, a non-thread-safe object) cannot be detected, so it is your responsibility.
Side effects inside lambdas (logging, metrics) can run out of order, or on elements past a
`take` or after an error, because upstream chunks are processed ahead. Parallelism pays off only
when each element does real work; for a cheap `map`, the hand-offs cost more than they save.

**Execution.** fs2 uses `parEvalMap` (ordered) and ZIO uses `mapZIOPar` (ordered), each over a
pool of reusable compiled instances, one task per chunk (not per element). For plain threads,
`strom-loom` runs the same plans over bounded queues: build one with
`LoomParallel.plan((a: Strom[Int]) => …)` and start it with
`LoomParallel.start(plan, in, out, Config(parallelism, queueCapacity, …, compute))`. Coordination
runs on virtual threads. Workers run on `compute`: virtual threads by default, or e.g.
`Thread.ofPlatform().factory()` for a dedicated pool. A semaphore-bounded window feeds an
`InFlight` / `ReorderBuffer` pair, so memory stays bounded. If a downstream tier finishes early
(`take`), the upstream tiers are stopped.

| Run shape | Plan |
|---|---|
| only stateless ops | `Replicated`: N instances, one chunk each, outputs reordered |
| stateless ops + associative fold | `PartialFold`: per-chunk folds combined in input order |
| stateless prefix, then a stateful op | `Replicated` prefix, then `Single`: cut at the stateful op |
| …then more stateless ops | light ones (< 3 ops, no `flatMap`) stay in the `Single` tier; heavy ones get another `Replicated` (or `PartialFold`) tier; adjacent `Single` tiers merge |

The semantics are checked by `Fs2ParallelSuite` and `ZioParallelSuite` (`fusedPar` against the
plain, unfused stream, for random inputs, chunkings and parallelism), `ParallelPlanSuite` (a
sequential executor that still round-robins over several instances) and `LoomParallelSuite`
(random parallelism, queue sizes, budgets and thread kinds).

## Lower-level API: the `Strom` DSL

`fused` compiles through a small DSL, also usable directly, e.g. for array processing or to drive
a segment by hand. Pipelines must be written inline (directly or via `inline def`).

```scala
import strom.Strom

// One fused loop over arrays, no allocation at all.
val r: Int = Strom.run {
  Strom.fromArray(a).flatMap(x => Strom.range(0, x))
    .zipWith(Strom.fromArray(b).filter(_ % 2 == 0))(_ * _)
    .take(n).sum
}

// A resumable segment; its typed lambda parameters are input ports (1 to 4).
val seg = Strom.segment((a: Strom[Int], b: Strom[Int]) =>
  a.flatMap(x => Strom.range(0, x)).zipWith(b.filter(_ % 2 == 0))(_ * _).take(n).sum)
val out: Stream[IO, Int] = StromFs2.fold2[IO, Int, Int, Int](seg)(as, bs)

LoomParallel.plan((a: Strom[Int]) => …) // a parallel plan, run on threads by strom-loom
Strom.showCode(…)                         // the generated code, as a compile-time string
Strom.explain(…)                          // stage metadata and the Kiselyov IR shape
```

## Guarantees

* **Nothing is allocated per element on the fused path.** User lambdas are beta-reduced into
  the loop body (no closures, no `Function1.apply`, no boxing); element types are specialised
  from `Type[A]` (primitive locals, primitive slot fields, primitive I/O arrays); there are no
  intermediate chunks. `AllocationSuite` measures this with the JVM's per-thread allocation
  counter. A segment run over 2M input elements allocates < 4 KB in total (the instance and
  one boxed result). A direct run allocates nothing.
* **Segments are synchronous, effect-free and thread-confined.** `step()` does at most `budget`
  producer steps and emits at most `maxOut` elements, then returns a status: `NeedInput(port)`,
  `Emit`, `Yield` or `Done`. Suspension only happens before an element is consumed or after
  it has been saved in state, so re-entering continues exactly where the step stopped.
* **A failing step poisons the instance**: every later `step()` rethrows.
* **`strom-core` has no dependencies** beyond the Scala 3 library. Runtime-specific code lives in
  adapter modules: `strom-fs2` (Pull + StepLeg, `cede`, `Chunk.array`), `strom-zio`
  (`ZChannel.readWithCause`, `yieldNow`, `Chunk.fromArray`) and `strom-loom` (virtual threads,
  blocking queues, `Thread.interrupted()` polling, `Thread.yield()` on budget exhaustion).

## How it works

| Layer | File | What |
|---|---|---|
| DSL | `Strom.scala` | Ordinary immutable trees (so the reference interpreter can run them); the staged entry points are macros that read the *source* of the expression. |
| Frontend | `internal/Frontend.scala` | Quoted patterns → `Pipe` IR. Lambdas are substituted (beta-reduced) and ports are lambda parameters. |
| Logical IR | `internal/Pipe.scala` | Ops plus static metadata: stateless / cardinality / associativity (for the parallel planner). |
| Kiselyov IR | `internal/Stream.scala` | `Producer = For(upb, index) \| Unfold(term, card, step)` with CPS `init`; `Stream = Linear \| Nested`. |
| Codegen | `internal/Gen.scala`, `Codegen.scala` | The staged combinators for every operator, let-insertion, `take` pushed into every loop, two backends (below), method splitting. |
| Runtime | `runtime/Segment.scala`, `Slots.scala`, `Runner.scala`, `Chunks.scala` | Segment base class (ports, output buffer, status codes, poisoning), slot fields, sans-I/O runner, reorder buffer, in-flight accounting, chunk-at-a-time operations and the instance pool shared by the parallel executors. |

Two backends share the IR:

* **`nest`** is Kiselyov's `fold_raw`: properly nested `while` loops over hoisted locals. Used
  for direct pipelines.
* **`flags`** is `make_linear` generalised. Every nesting level has an `active` flag, and all
  state lives in storage (hoisted locals, or **slot fields** of the segment: "locals →
  fields"), so the loop nest can be left at any element boundary and re-entered from the top.
  It is used for zip/window operands (pulls) and for every loop of a resumable segment.

Zip picks the cheapest correct strategy, following strymonas:
* a shared index for two indexed producers;
* lock-step stepping for two exactly-one producers (stepping the suspendable one first);
* when one side yields exactly one element per step, the *other* side drives in its own nested
  loops and this side is stepped inside;
* otherwise only the shallower side becomes a pull, and the other side keeps its loops.

`mapAccumFilter(z)((s, a) => (s', Option[b]))` is strymonas' Mealy machine (`map_accum_filter`):
the step's tuple and `Some`/`None` are taken apart at compile time, through `if`/`else`.

**Method splitting.** HotSpot does not JIT-compile methods over 8000 bytes of bytecode. The
generator measures its output (≈1 byte of bytecode per tree node, calibrated against class
files) and, when the estimate exceeds 6000 bytes, regenerates with the largest regions
outlined into local `def`s. The compiler lifts these into separate methods, and `return`s
inside them are retargeted. Direct pipelines that are too large switch to field storage on a
one-shot frame, so that outlined code does not capture (and box) locals.

## Verification

* `SegmentSuite`, `DirectSuite`, `RunnerSuite`, `StromFs2Suite`: fixed pipelines (the
  representative one, nested, zip of ports/ranges/pulls, windows, take everywhere, Long /
  Double / Byte / String / tuple elements, closures inside lambdas) agree with `Reference`
  for random inputs, **random chunkings** (including empty chunks and offsets), random output
  capacities and random work budgets down to 1.
* `StagedShapesSuite`: **random pipeline shapes**, compiled at test time with
  `scala.quoted.staging` by the same generator, checked against the reference interpreter.
  Default: 200 shapes × 40 runs. `STROM_SHAPE_BATCHES=80` gives 2000 shapes.
* `GoldenSuite`: golden files of `Expr.show` (`STROM_UPDATE_GOLDEN=1` to update).
* `MethodSizeSuite`: parses the compiled class files and asserts that no generated method
  reaches 8000 bytes. It includes deliberately huge pipelines that must be split.
* `SafetySuite`: poisoning, bounded work, bounded output, early termination.
* `AllocationSuite`, `TupleSuite`: zero per-element allocation (measured with the JVM's
  allocation counter), and no `Tuple2` / `Some` in the generated code for destructured tuples and
  `mapAccumFilter` steps.
* `FusedSuite`, `ZioFusedSuite`, `Fs2ParallelSuite`, `ZioParallelSuite`: `fused` / `fusedPar`
  against the same plain fs2 / ZIO code, unfused.
* `ParallelPlanSuite`, `LoomParallelSuite`: parallel plans against the reference interpreter.

`sbt test` runs everything.

## Numbers

Measured with JMH on an Apple M1 (4 performance + 4 efficiency cores), JDK 23.
**Lower is better in every table.** The rows come from a few separate runs.

Benchmarks (they load every core, so run them on an otherwise idle machine):

* `sbt "bench/Jmh/run -prof gc strom.bench.CheapStages strom.bench.Representative"`: ns and bytes per element.
* `sbt "bench/Jmh/run strom.bench.Scaling"`: 1/2/4/8-way replicated tier (loom, fs2, ZIO) vs sequential.
* `sbt "bench/runMain strom.bench.Latency"`: lateness of a 1 ms ticker fiber while every core
  runs fused pipelines (with cede, without yields, direct loop, dedicated pool).
* `sbt "bench/runMain strom.bench.check"`: all variants compute the same result.

### Throughput: how fast and how much garbage

Each cell is *nanoseconds per element / bytes allocated per element*. Time per element is the
pipeline machinery's cost. Bytes per element is garbage the GC must collect later; 0 is the goal,
because garbage costs GC pauses on top of the time shown.

| | A: 7 cheap stages + sum | B: flatMap + zip + filter + take + sum |
|---|---|---|
| hand-written `while` loop (the floor) | 4.6 / 0 | — |
| `Strom.run` (direct) | 4.7 / 0 | 3.3 / 0 |
| Ström segment, raw runner | 5.1 / 0 | 4.1 / 0 |
| Ström via ZIO adapter | 5.7 / 4.3 | — |
| Ström via fs2 adapter | 5.9 / 1.4 | 7.4 / 23 |
| **plain fs2 code wrapped in `StromFs2.fused`** | **6.2 / 1.4** | **8.3 / 36** |
| plain fs2 | 52 / 108 | 825 / 3269 |
| plain ZStream | 31 / 147 | 641 / 1519 |
| Scala iterator | 74 / 80 | — |

How to read it:
* **Good:** fused code is within ~1.5 ns of a hand-written loop, the best possible. Wrapping
  ordinary fs2 code in `fused` makes pipeline A ~8× faster and pipeline B ~100× faster, with
  ~1 byte of garbage per element instead of 100+.
* **Why B gains so much:** plain `flatMap` + `zip` builds many small streams and pulls per element.
  Fusion turns them into nested loops.
* **Where the remaining cost goes:** the fs2 path is ~1.3× the raw loop: per-chunk adapter work
  (copying output chunks, the effect system's step loop). Its allocation is per chunk, not per element.
* **What it means for an app:** the gain applies to the time spent *in stream machinery*.
  - CPU-heavy pure pipelines: large gains.
  - Pipelines dominated by I/O or effects (`evalMap`, DB calls): small gains, though less garbage
    still helps tail latency.

### Scaling: does it use more cores?

Nanoseconds per element for one stateless pipeline (~20 operations per element) split over N
workers. Lower is better; halving it when N doubles would be perfect scaling.

| workers | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| loom executor | 8.0 | 4.1 | 2.2 | 1.9 |
| fs2 (`fusedPar`) | 8.3 | 4.7 | 2.9 | 2.8 |
| single thread, no executor | 7.4 | | | |

* **Good:** near-perfect scaling up to 4 workers (8.0 → 2.2 ns, ~3.6×). The executor's own
  overhead is small: 1 worker ≈ the single-thread baseline.
* **Limited:** 4 → 8 adds little. The M1's other 4 cores are slower efficiency cores, and the
  queue hand-offs start to matter (not profiled further). fs2 flattens earlier (more coordination per chunk).
* Measured before the 100 µs time-slice change, with the executors that `fusedPar` now uses.
  ZIO scaling (`zioPar`) and partial-fold / multi-cut plans have not been benchmarked yet.

### Responsiveness: does fused work starve other tasks?

A timer fiber sleeps 1 ms in a loop while all 8 cores run fused pipelines on the cats-effect
runtime. The table shows how late the timer wakes up. Lower is better: lateness is the delay any
request or I/O callback would see.

| load | typical (p50) | worst 1% (p99) |
|---|---|---|
| no load (baseline) | 0.52 ms | 0.63 ms |
| Ström default: yields every 100 µs | 0.13 ms | 2.9 ms |
| Ström told never to yield | 1.4 s | 2.8 s |
| one fused loop with no yield points at all | starved: ~2 ticks in 110 s | |
| Ström on a dedicated thread pool (`evalOn`) | 0.53 ms | 1.7 ms |

* **Good:** with the default settings other work stays responsive (p99 ≈ 3 ms on a completely
  saturated machine). Without yielding, the same work blocks the timer for seconds.
* **Best:** running fused work on its own thread pool keeps latency close to an idle machine.
* **How:** a segment returns to its runner every `budget` steps (default 1024). The runner hands
  control back to the scheduler only once per 100 µs slice, because each hand-off costs a few
  microseconds. That keeps throughput unchanged.

## Limits (v1)

* The "heavy" rule for re-replicating after a stateful operator is a fixed heuristic (no cost
  model). Associative folds are recognised only for the forms listed in
  [Running on several cores](#running-on-several-cores).
* Inner streams of a fused `flatMap` must be pure sources (`range`, `emit`, `emits`, `chunk`, …)
  plus pure operators; any other `flatMap` is kept as written. Generic library collections may
  box primitives when read (e.g. ZIO `Chunk.apply`).
* Not fused (by design): fan-out (an input port is consumed once), effects inside segments,
  dynamic pipeline shapes. Those stay at the runtime level, between segments.
* `flatMap` needs a lambda literal whose body is a `Strom` expression. Other functions may be
  non-literal (Ström then calls them through `Function.apply` and warns).
* Pipelines must be written inline (directly, or via `inline def`).
* At most 4 input ports; the state slot pool (96 Int / 32 Long / 32 Double / 16 Float /
  96 Boolean / 48 reference slots) bounds pipeline size. Exceeding it is a compile error.
* Tuples from `zip` / `zipWithIndex` / user lambdas are not allocated when they are only projected
  (`_._1`), destructured (`(x, i) => …`) or matched with tuple patterns (`collect { case (x, y) if … }`);
  a tuple that escapes (emitted, folded, stored across a pull or `flatMap`) is still allocated.
* `collect` is staged as a `match` only for partial-function literals; others go through
  `isDefinedAt` / `apply`.

## Relation to strymonas

Ström follows strymonas [1, 2]. From the later normalization-by-evaluation version [2] it adopts:
* zipping by driving with the complex side and linearising only the simple one;
* elements as staging-time values, so tuples never exist at runtime unless they escape;
* `map_accum_filter` as the general stateful operator.

It adds resumable, chunk-driven segments, a sans-I/O runner with fs2 / ZIO / loom adapters, a
parallel tier, and fusion of plain fs2 / ZIO code. Correctness rests on property tests against a
reference interpreter, not on mechanised proofs (strymonas proves its laws in Agda).

## References

1. Oleg Kiselyov, Aggelos Biboudis, Nick Palladinos, Yannis Smaragdakis. *Stream Fusion, to
   Completeness*. POPL 2017. [doi:10.1145/3009837](https://doi.org/10.1145/3009837)
2. Oleg Kiselyov, Tomoaki Kobayashi, Nick Palladinos. *Complete Fusion for Stateful Streams:
   Equational Theory of Stateful Streams and Fusion as Normalization-by-Evaluation*. TOPLAS.
   [arXiv:2412.15768](https://arxiv.org/abs/2412.15768). Implementation, derivations and Agda
   proofs: [github.com/strymonas/strymonas-ocaml](https://github.com/strymonas/strymonas-ocaml).
3. Tomoaki Kobayashi, Oleg Kiselyov. *Complete Stream Fusion for Software-Defined Radio*.
   PEPM 2024. [doi:10.1145/3635800.3636962](https://doi.org/10.1145/3635800.3636962)

## License

MIT, see [LICENSE](LICENSE).
