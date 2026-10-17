# iofuse (prototype)

*AI-generated code and docs (Claude). Experimental. Not part of Ström and not published.*

Wrap a method's body in `IOFuse.fused`, and its effect code is compiled into plain Scala. Your
code stays the same, and so do its results. It just runs faster.

```scala
import iofuse.*

def checkout(order: Order): IO[Receipt] = IOFuse.fused {
  for
    _     <- if order.items.isEmpty then IO.raiseError(Rejected("empty")) else IO.unit
    user  <- users.find(order.user)                 // a service call: stays a call
    total <- priceAll(order.items, 0L)              // a local def: compiled too
    disc   = if user.vip then total / 10 else 0L
    _     <- sales.record(total - disc)
  yield Receipt(order.id, total - disc)
}
```

Works with cats-effect `IO` and ZIO.

## Where to put it

**Put it on each method you want faster: a request handler, a processing loop, a validation
step.** One `fused` at the top of your program does not reach down into the methods it calls.
iofuse compiles only the code written inside the block, including local `def`s. A call to
another method stays an ordinary call; wrap that method too if you want it fused.

**You don't have to choose carefully.** If a block has nothing to gain (only service calls and
the `flatMap`s between them), iofuse leaves it exactly as you wrote it.

## What you get

- **Synchronous effect code is 2× to 60× faster** and stops allocating. This covers loops,
  validation and error recovery, and anything that is `pure` / `delay` / `map` / `flatMap` /
  `handleErrorWith` between real effects.
- **Service code that is mostly calls is a few percent faster**, because the calls themselves are
  not touched.
- **Nothing measured is slower.**
- **The behaviour is the same:** same values, same errors, same order of effects, same
  cancelation and fairness. It is checked by differential tests and fuzzers on both libraries.

## Benchmarks

JMH, Apple M1, time per operation, plain → fused.

| Program | cats-effect | ZIO |
|---|---|---|
| cats-effect's `HandleError` benchmark | 57–72 → 1.2 ns (**48–60×**) | |
| cats-effect's `DeepBind` / `ShallowBind` (pure, delay) | 11–17 → 1.2 ns (**8–13×**) | |
| cats-effect's `DeepBind` / `ShallowBind` (with `cede`) | 46–50 → 25 ns (**2×**) | |
| cats-effect's `Attempt` benchmark | 21–46 → 15 ns (**1.5–3×**) | |
| six synchronous steps in a loop | 112 → 2.2 ns (**51×**) | 115 → 2.5 ns (**46×**) |
| a step that fails 1 in 10, recovered | 461 → 35 ns (**13×**) | 47 → 3.1 ns (**15×**) |
| sync steps + one `Ref.update` per iteration | 55 → 21 ns (**2.6×**) | 63 → 29 ns (**2.2×**) |
| order checkout handler (service calls, a `Ref`, validation) | 446 → 420 ns (**6%**) | 376 → 348 ns (**8%**) |
| only service calls (`Ref.modify` loop, `IO.both`) | unchanged | unchanged |

The first four rows are cats-effect's own run-loop benchmarks, copied as written. The checkout
handler is a realistic request handler: it validates the order, looks the user up, prices the line
items, reserves stock in a `Ref` and records the sale. All of it is in
[bench/](bench/src/main/scala/iofuse/bench/): run `iofuseBench/Jmh/run -prof gc`.

With real I/O (database or network calls at tens of microseconds each), handler gains shrink in
proportion. The large gains are in code that does real work between its effects.

## Details

- **What is fused:**
  - cats-effect: `pure`, `delay`, `unit`, `raiseError`, `map`, `flatMap`, `*>`, `>>`, `as`,
    `void`, `handleErrorWith` and `attempt`;
  - ZIO: `succeed`, `fail`, `die`, `attempt`, `map`, `flatMap`, `*>`, `as`, `catchAll` and
    `either`;
  - on both: `if` and `match`, and local `def`s, which become `while` loops when they recurse.

  Everything else is left as a call: your methods, `Ref`, `sleep`, `IO.both`, masks. The effects
  you pass to such calls are fused too.
- **Not fused:** code written against `F[_]: Sync`, and methods called from the block (wrap them
  too).
- **Fairness:** a fused loop yields every 1024 iterations, as cats-effect does, so it stays fair
  and cancelable.
- **Seeing the result:** `IOFuse.show { ... }` prints the generated code.
- **Debugging:** on cats-effect, fused steps don't show up in async traces. Compile with
  `-Diofuse.fuse=off` to turn fusing off everywhere. ZIO traces still name your lines.
- **Compile-time notes:** a rare case is a local def whose body starts with a `var`, a lazy val or
  another def. There, the compiler notes where its build order could differ from the plain code.
  `-Diofuse.notes=off` silences the notes.

## References

- O. Kiselyov, A. Biboudis, N. Palladinos, Y. Smaragdakis. *Stream Fusion, to Completeness.*
  POPL 2017. The staging idea behind Ström, applied here to effect code.
- R. Elizarov, M. Belyaev, M. Akhin, I. Usmanov. *Kotlin Coroutines: Design and Implementation.*
  Onward! 2021. Also the [coroutines design document](https://github.com/Kotlin/KEEP/blob/master/proposals/coroutines.md).
  Its state machines on one continuation object, with spilled locals cleared once they are dead,
  are the model for iofuse's optional frames (`fusedWith(.., frames = true)`).
- [cats-effect benchmarks](https://github.com/typelevel/cats-effect/tree/v3.7.1/benchmarks/src/main/scala/cats/effect/benchmarks)
  (v3.7.1, Apache 2.0). Their programs are copied, unchanged, in `CEBench.scala`.
- ZIO's runtime runs the start of a program on the calling thread. The benchmarks therefore start
  each ZIO program with `ZIO.yieldNow`, so that ZIO and cats-effect are both measured on their
  own thread pools.
