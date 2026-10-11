package strom.zio

import _root_.zio.stream.ZStream

/** Postfix syntax, `import strom.zio.*`:
 *  {{{ in.map(f).filter(p).mapZIO(save).map(g).fused }}}
 *  The receiver is the whole expression to its left, as with [[StromZio.fused]]. */
extension [S <: ZStream[?, ?, ?]](inline stream: S)
  /** [[StromZio.fused]] as a method. */
  inline def fused: S = ${ FusedMacro.fused[S]('stream) }
  /** [[StromZio.fusedPar]] as a method. */
  inline def fusedPar(parallelism: Int): S = ${ FusedMacro.fusedPar[S]('parallelism, 'stream) }
  /** [[StromZio.fusedPlan]] as a method. */
  inline def fusedPlan: String = ${ FusedMacro.plan('stream) }
