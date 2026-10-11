package strom.fs2

import _root_.fs2.Stream

/** Postfix syntax, `import strom.fs2.*`:
 *  {{{ in.map(f).filter(p).evalMap(save).map(g).fused }}}
 *  The receiver is the whole expression to its left, as with [[StromFs2.fused]]. */
extension [S <: Stream[?, ?]](inline stream: S)
  /** [[StromFs2.fused]] as a method. */
  inline def fused: S = ${ FusedMacro.fused[S]('stream) }
  /** [[StromFs2.fusedPar]] as a method. */
  inline def fusedPar(parallelism: Int): S = ${ FusedMacro.fusedPar[S]('parallelism, 'stream) }
  /** [[StromFs2.fusedPlan]] as a method. */
  inline def fusedPlan: String = ${ FusedMacro.plan('stream) }
