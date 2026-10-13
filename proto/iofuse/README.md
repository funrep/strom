# iofuse (prototype)

*AI-generated code and docs (Claude). Experimental. Not part of Ström and not published.*

Ström fuses streams. This tries the same idea on ordinary `IO` code. `IOFuse.fused { ... }` (or
`.fused`) compiles a block of cats-effect `IO` into plain Scala, and keeps `IO` only where it is
needed.

```scala
import iofuse.*

val program: IO[Long] = IOFuse.fused {
  def loop(i: Int, acc: Long): IO[Long] =
    if i == n then IO.pure(acc)
    else
      for
        a <- IO(i * 7)
        b <- if a % 3 == 0 then IO.pure(a + 1) else repo.lookup(a)   // repo.lookup is opaque
        r <- loop(i + 1, acc + b)
      yield r
  loop(0, 0L)
}
```

## How it works

1. **Parse.** The macro recognises a fixed set of operations:
   - `pure`, `delay`/`IO(...)`, `unit`, `raiseError`, `defer`;
   - `map`, `flatMap`, `*>`, `>>`, `<*`, `as`, `void`;
   - `if`, `match`, `handleErrorWith`, `attempt`;
   - local `def`s returning `IO`.

   Any other `IO` value is **opaque**: a method call, `ref.update`, `IO.sleep`, and so on.
2. **Synchronous parts become direct code.** `IO(x * 7).map(_ + 1)` becomes
   `val v = x * 7; v + 1`, with no IO nodes and no closures.
3. **An opaque call ends the synchronous run** with `opaque.flatMap(v => rest)`. The runtime still
   handles everything there: async boundaries, cancelation, masks.
4. **Loops.** A local `def` that calls itself in tail position becomes a `while` loop. It runs
   synchronously until it reaches an opaque call. It calls `IO.cede` every 1024 iterations, which
   matches cats-effect's default auto-yield (cancelation checks every 512, auto-yield every 1024).
5. **Errors.**
   - In synchronous code, a throw or `raiseError` becomes `throw`, and the runtime turns it into an
     `IO` error.
   - `handleErrorWith` and `attempt` become `try`/`catch` around synchronous code, and a real
     `handleErrorWith` around opaque code.
6. **`yield r` (identity `map`) is dropped**, so a loop at the end of a for-comprehension stays a
   tail call.

`IOFuse.show { ... }` prints the generated code.

## Numbers

Each program loops 10 000 times. The numbers are ns and bytes allocated per iteration, from JMH with
`-prof gc` (`iofuseBench/Jmh/run -prof gc`). See [IOBench.scala](bench/src/main/scala/iofuse/bench/IOBench.scala).

| Program, per iteration | Plain IO | Fused |
|---|---|---|
| `syncLoop`: `delay` + `flatMap` | 16.8 ns / 96 B | 1.7 ns / 0 B |
| `forComp`: a six-step sync for-comprehension | 118 ns / 504 B | 2.3 ns / 0 B |
| `errors`: sync step, fails 1 in 10, recovered | 463 ns / 728 B | 35 ns / 73 B |
| `refLoop`: sync steps + one `Ref.update` | 57 ns / 328 B | 26 ns / 128 B |
| `opaqueOnly`: two opaque calls, nothing else | 45 ns / 168 B | 35 ns / 208 B |

How much it saves depends on the share of synchronous work. Opaque-only code gets a little faster
but allocates more, because each nested continuation captures the loop state. A single frame
object per run, with a program counter, would fix that. That is not built yet.

## Tests

[FusedSuite](src/test/scala/iofuse/FusedSuite.scala) runs every program both plain and fused, and
compares the result (or the error) and the effect log. It covers:

- loops of a million iterations;
- Ref-heavy loops;
- join points in `match`;
- non-tail and mutual recursion at a depth of 100 000;
- errors in every position;
- concurrent reruns of one fused value;
- fairness: two endless fused loops on one compute thread both make progress and `timeout` cancels
  them. This test hangs if the cede is removed.

## Semantics and limits

- **Construction is assumed pure.** An `IO`-typed expression may be *built* later than in the plain
  code (after earlier steps ran). For example, `a *> b` builds `b` after `a` runs. Running it is
  unchanged.
- **Visibility.** Only code written inside the block is compiled. These stay opaque:
  - non-`inline` methods;
  - `Ref`/`Deferred`;
  - `F[_]: Sync` code.
- **Concrete `IO` only.** ZIO isn't done yet.
- **Local defs** must have one parameter list and no type parameters. A block that contains other
  definitions (classes, polymorphic defs) is left unfused.
- **Cancelation and preemption** only happen at opaque calls and at the loop cedes. A synchronous
  stretch runs uninterrupted, like one big `IO.delay`.
- **Fatal errors:** `IO.raiseError(fatal)` becomes a `throw`, so the runtime treats it as fatal.

## Next

- Frame-based continuations, to cut allocation on opaque-heavy code.
- ZIO.
- A fuzzer like Ström's.
- `TestControl` tests around `sleep` and timeouts.
