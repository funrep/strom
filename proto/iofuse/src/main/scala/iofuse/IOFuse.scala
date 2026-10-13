package iofuse

import cats.effect.IO

/** Prototype: compile a block of plain cats-effect `IO` code into direct code.
 *
 *  Known synchronous steps (`pure`, `delay`, `map`, `flatMap`, `*>`, `if`, `match`, error handling)
 *  run as plain Scala. Anything else of type `IO` (a method call, a `Ref`, `sleep`, ...) is opaque:
 *  the code stops there and continues in a `flatMap`. Local `def`s returning `IO` that call
 *  themselves in tail position become `while` loops, which `IO.cede` every [[YieldEvery]] iterations.
 *
 *  The block must construct its IOs purely: an `IO`-typed expression may be evaluated later than in
 *  the plain code (after the preceding steps ran instead of when the chain was built). */
object IOFuse:
  /** Iterations of a compiled loop between `IO.cede`s (cats-effect auto-yields every 1024 steps). */
  final val YieldEvery = 1024

  inline def fused[A](inline io: IO[A]): IO[A] = ${ internal.Compiler.fused('io) }

  /** The generated code, for debugging. */
  inline def show[A](inline io: IO[A]): String = ${ internal.Compiler.show('io) }

extension [A](inline io: IO[A])
  inline def fused: IO[A] = ${ internal.Compiler.fused('io) }
