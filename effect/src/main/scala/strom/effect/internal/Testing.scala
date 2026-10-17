package strom.effect.internal

import cats.effect.IO
import zio.ZIO

/** For strom-effect's own tests: `fused` with another yield interval, and with `gate = false`
 *  compiling every block and def, also those that fusing would save nothing on (which `fused`
 *  leaves as written), so the fuzzers test the compiler on all of their programs. */
object Testing:
  inline def fusedWith[A](inline yieldEvery: Int, inline gate: Boolean)(inline io: IO[A]): IO[A] =
    ${ Compiler.fusedIOWith('io, 'yieldEvery, 'gate) }
  inline def fusedWith[R, E, A](inline yieldEvery: Int, inline gate: Boolean)(inline z: ZIO[R, E, A]): ZIO[R, E, A] =
    ${ Compiler.fusedZIOWith('z, 'yieldEvery, 'gate) }
  inline def showWith[A](inline yieldEvery: Int)(inline io: IO[A]): String =
    ${ Compiler.showIOWith('io, 'yieldEvery) }
