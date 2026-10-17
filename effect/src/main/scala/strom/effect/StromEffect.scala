package strom.effect

import cats.effect.IO
import zio.ZIO

/** Compile a block of plain cats-effect `IO` (or ZIO) code into direct code.
 *
 *  Known synchronous steps (`pure`, `delay`, `map`, `flatMap`, `*>`, `if`, `match`, error handling)
 *  run as plain Scala. Anything else of the effect type (a method call, a `Ref`, `sleep`, ...) is
 *  opaque: the code stops there and continues in a `flatMap`. Local `def`s returning the effect
 *  that call themselves in tail position become `while` loops, which cede every [[YieldEvery]]
 *  iterations. A block or local def that fusing would save nothing on (only opaque calls and the
 *  glue between them) is left as written.
 *
 *  What the plain code evaluates while it builds an effect (opaque effect expressions, by-value
 *  arguments, block statements, the start of a local def's body when the def is called) is
 *  evaluated at the same moment by the fused code. The rare exceptions are reported as compile-time
 *  notes ([[notes]]). */
object StromEffect:
  /** Iterations of a compiled loop, and steps of a synchronous stretch, between cedes
   *  (cats-effect auto-yields every 1024 steps). */
  final val YieldEvery = 1024

  inline def fused[A](inline io: IO[A]): IO[A] = ${ internal.Compiler.fusedIO('io) }
  inline def fused[R, E, A](inline z: ZIO[R, E, A]): ZIO[R, E, A] = ${ internal.Compiler.fusedZIO('z) }

  /** The generated code, for debugging. */
  inline def show[A](inline io: IO[A]): String = ${ internal.Compiler.showIO('io) }
  inline def show[R, E, A](inline z: ZIO[R, E, A]): String = ${ internal.Compiler.showZIO('z) }

  /** The compiler's notes for the block: calls of a local def that the fused code builds later
   *  than the plain code. */
  inline def notes[A](inline io: IO[A]): List[String] = ${ internal.Compiler.notesIO('io) }
  inline def notes[R, E, A](inline z: ZIO[R, E, A]): List[String] = ${ internal.Compiler.notesZIO('z) }

extension [A](inline io: IO[A])
  inline def fused: IO[A] = ${ internal.Compiler.fusedIO('io) }

extension [R, E, A](inline z: ZIO[R, E, A])
  inline def fused: ZIO[R, E, A] = ${ internal.Compiler.fusedZIO('z) }
