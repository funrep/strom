package strom.bench

/** Sanity check: all variants of each benchmark compute the same result. */
@main def check(): Unit =
  val c = CheapStages()
  val cs = List(c.whileLoop(), c.stromDirect(), c.stromRunner(), c.stromFs2(), c.stromZio(), c.fs2Fused(), c.fs2FusedPar4(), c.zioFused(), c.zioFusedPar4(), c.fs2Plain(), c.zstream(), c.scalaIterator())
  println(s"CheapStages: $cs"); assert(cs.distinct.size == 1)
  val r = Representative()
  val rs = List(r.whileLoop(), r.stromDirect(), r.stromRunner(), r.stromFs2(), r.fs2Fused(), r.fs2FusedPar4(), r.zioFused(), r.zioFusedPar4(), r.fs2Plain(), r.zstream())
  println(s"Representative: $rs"); assert(rs.distinct.size == 1)
