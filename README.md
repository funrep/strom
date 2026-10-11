# Ström

Guaranteed, allocation-free stream fusion for Scala 3, by staging (quotes), after Kiselyov,
Biboudis, Palladinos and Smaragdakis, *Stream Fusion, to Completeness* (POPL 2017).

A pipeline written with the `Strom` DSL is compiled **at compile time** into loops: either one
fused loop over arrays (`Strom.run`) or a **resumable, chunk-driven segment** that a
runtime-agnostic, sans-I/O runner drives one bounded step at a time.

```scala
import strom.Strom

// Direct: one fused loop, no allocation at all.
val r: Int = Strom.run {
  Strom.fromArray(a).flatMap(x => Strom.range(0, x))
    .zipWith(Strom.fromArray(b).filter(_ % 2 == 0))(_ * _)
    .take(n).sum
}

// Resumable segment with two input ports, driven by fs2:
val seg = Strom.foldSegment2((a: Strom[Int], b: Strom[Int]) =>
  a.flatMap(x => Strom.range(0, x)).zipWith(b.filter(_ % 2 == 0))(_ * _).take(n).sum)
val out: Stream[IO, Int] = StromFs2.fold2[IO, Int, Int, Int](seg)(as, bs)

Strom.showCode(...)   // the generated code, as a compile-time string
Strom.explain(...)    // stage metadata and the Kiselyov IR shape
```

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
| Codegen | `internal/Gen.scala` | `map / filter / flatMap / take / zipWith / windowFold`, let-insertion, `take` pushed into every loop, two backends (below), method splitting. |
| Runtime | `runtime/Segment.scala`, `Slots.scala`, `Runner.scala` | Segment base class (ports, output buffer, status codes, poisoning), slot fields, sans-I/O runner, key partitioner, reorder buffer, in-flight accounting. |

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
default arguments (`takeWhile(p)`) are recognised. `examples/` is a standalone project that uses the
published artifacts on realistic fs2 / ZIO pipelines.

| | fs2 | ZIO |
|---|---|---|
| fused operators | `map`, `filter`, `collect`, `mapFilter` (cats syntax), `take`, `drop`, `takeWhile`, `dropWhile`, `zipWithIndex`, `scan`, `mapAccumulate`, `fold`, `zip`, `zipWith`, `flatMap` onto `Stream.range` | `map`, `filter`, `collect`, `take`, `drop`, `takeWhile`, `dropWhile`, `zipWithIndex`, `scan`, `mapAccum`, `zipWith`, `flatMap` onto `ZStream.range` |
| inputs per run | up to 4 (`zipWith`; a pure `zipWith` operand is inlined instead) | up to 4 |
| yields to the scheduler | `cede` when a `Spawn[F]` exists (not for `Pure`) | `yieldNow` |

A run must be worth a segment boundary: two or more operators, or a `flatMap`, `zipWith` or `fold`.
The library-independent part lives in `strom.internal.ChainFuser`; each adapter only says
which calls are pure operators and how to run a segment.

## Relation to strymonas

Ström follows strymonas [1, 2]. From the later normalization-by-evaluation version [2] it adopts:
* zipping by driving with the complex side and linearising only the simple one;
* elements as staging-time values, so tuples never exist at runtime unless they escape;
* `map_accum_filter` as the general stateful operator.

It adds resumable, chunk-driven segments, a sans-I/O runner with fs2 / ZIO / loom adapters, a
parallel tier, and fusion of plain fs2 / ZIO code. Correctness rests on property tests against a
reference interpreter, not on mechanised proofs (strymonas proves its laws in Agda).

## Parallel tier

`Strom.parallel` / `parallelFold` / `parallelKeyed` compile a one-input pipeline into a
`ParallelPlan`, using the static metadata (stateless / associative / cardinality):

| Pipeline shape | Plan |
|---|---|
| only stateless ops (`map`, `filter`, `flatMap`) | `Replicated`: N instances, one chunk each, outputs reordered (`ReorderBuffer`) |
| stateless ops + `sum` / `count` / `foldAssociative` (element = result type) | `PartialFold`: per-chunk folds combined in input order |
| stateless prefix, then a stateful op (`take`, `zipWith`, `windowFold`, `scan`, other folds…) | `Replicated` prefix, then `Single`: the pipeline is cut at the stateful op |
| …then more stateless ops | light ones (< 3 ops, no `flatMap`) stay fused in the `Single` tier; heavy ones get another `Replicated` (or `PartialFold`) tier; adjacent `Single` tiers merge |
| `parallelKeyed(key)(…)` | `Keyed`: one instance per partition; a staged partitioner (key function inlined, specialised to the element type) routes each chunk |

Plans are pure data. The executors live in the adapters:

* `LoomParallel.start(plan, in, out, Config(parallelism, queueCapacity, …, compute))`: tiers
  connected by bounded queues. Coordination runs on virtual threads. Workers run on `compute`
  (virtual by default; e.g. `Thread.ofPlatform().factory()` for a dedicated pool). A
  semaphore-bounded window feeds an `InFlight` / `ReorderBuffer` pair, so memory stays bounded.
  If a downstream tier finishes early, the upstream tiers are stopped.
* `StromFs2.parallel(plan, parallelism, compute = Some(ec))`: `parEvalMap` (ordered) over a
  pool of reusable instances. With `compute`, CPU work runs via `evalOn`. Keyed tiers use one
  bounded `Channel` per partition and `parJoin`.
* `StromZio.parallel(plan, parallelism, compute = Some(executor))`: `mapZIOPar` (ordered) over a
  pool of instances, `onExecutor` for CPU work, bounded queues per partition for keyed tiers.

The semantics are checked by `ParallelPlanSuite` (a sequential executor that still
round-robins over several instances), `LoomParallelSuite`, `Fs2ParallelSuite` and `ZioParallelSuite` (random
parallelism, queue sizes, budgets and thread kinds), all against the reference interpreter.

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
* `ParallelPlanSuite`, `LoomParallelSuite`, `Fs2ParallelSuite`, `ZioParallelSuite`, `FusedSuite`,
  `ZioFusedSuite`: parallel plans and fusion of plain fs2 / ZIO code against the reference.

`sbt test` runs everything.

Benchmarks (they load every core, so run them on an otherwise idle machine):

* `sbt "bench/Jmh/run -prof gc strom.bench.CheapStages strom.bench.Representative"`: ns and bytes per element.
* `sbt "bench/Jmh/run strom.bench.Scaling"`: 1/2/4/8-way replicated tier (loom, fs2, ZIO) vs sequential.
* `sbt "bench/runMain strom.bench.Latency"`: lateness of a 1 ms ticker fiber while every core
  runs fused pipelines (with cede, without yields, direct loop, dedicated pool).
* `sbt "bench/runMain strom.bench.check"`: all variants compute the same result.

## Numbers

Measured with JMH on an Apple M1 (4 performance + 4 efficiency cores), JDK 23.
**Lower is better in every table.** The rows come from a few separate runs.

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
| fs2 executor | 8.3 | 4.7 | 2.9 | 2.8 |
| single thread, no executor | 7.4 | | | |

* **Good:** near-perfect scaling up to 4 workers (8.0 → 2.2 ns, ~3.6×). The executor's own
  overhead is small: 1 worker ≈ the single-thread baseline.
* **Limited:** 4 → 8 adds little. The M1's other 4 cores are slower efficiency cores, and the
  queue hand-offs start to matter (not profiled further). fs2 flattens earlier (more coordination per chunk).
* Measured before the 100 µs time-slice change; ZIO scaling (`zioPar`) has not been run yet.

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

* Parallel plans support one-input pipelines; the "heavy" rule for re-replicating after a
  stateful operator is a fixed heuristic (no cost model).
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

## References

1. Oleg Kiselyov, Aggelos Biboudis, Nick Palladinos, Yannis Smaragdakis. *Stream Fusion, to
   Completeness*. POPL 2017. [doi:10.1145/3009837](https://doi.org/10.1145/3009837)
2. The later strymonas paper: a theory of stateful streams, with fusion by normalization-by-evaluation,
   linear normal forms for zip, closure conversion of nested streams, and the equational laws
   proved in Agda. Implementation, derivations and proofs:
   [github.com/strymonas/strymonas-ocaml](https://github.com/strymonas/strymonas-ocaml)
   (`lib/derivation.ml`, `lib/Derivation.agda`, `lib/intuition.ml`).
3. Tomoaki Kobayashi, Oleg Kiselyov. *Complete Stream Fusion for Software-Defined Radio*.
   PEPM 2024. [doi:10.1145/3635800.3636962](https://doi.org/10.1145/3635800.3636962)

## License

MIT, see [LICENSE](LICENSE).
