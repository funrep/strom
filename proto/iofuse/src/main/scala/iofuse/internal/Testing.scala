package iofuse.internal

import cats.effect.IO
import zio.ZIO

/** For iofuse's own tests: `fusedWith` that compiles every block and def, also those that fusing
 *  would save nothing on (which `fused` leaves as written), so the fuzzers test the compiler on all
 *  of their programs. */
object Testing:
  inline def fusedAll[A](inline yieldEvery: Int, inline frames: Boolean)(inline io: IO[A]): IO[A] =
    ${ Compiler.fusedIOAll('io, 'yieldEvery, 'frames) }
  inline def fusedAll[R, E, A](inline yieldEvery: Int, inline frames: Boolean)(inline z: ZIO[R, E, A]): ZIO[R, E, A] =
    ${ Compiler.fusedZIOAll('z, 'yieldEvery, 'frames) }
