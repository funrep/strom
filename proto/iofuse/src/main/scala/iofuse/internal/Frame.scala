package iofuse.internal

/** The state of one run of a compiled loop or block that suspends, and the continuation the
 *  runtime calls to resume it: `apply(v)` runs `body` from state `pc` with the value `v`.
 *
 *  One object with plain fields. The values that live across a suspension are in slots: the first
 *  four Ints, two Longs and four references in fields, the rest in arrays; other primitives are
 *  boxed into reference slots. (State kept in several small objects, as captured `var`s are, made
 *  the same state machine twice as slow on ZIO.) */
final class Frame(body: (Frame, Any) => AnyRef, ints: Int, longs: Int, refs: Int) extends (Any => AnyRef):
  var pc: Int = 0
  /** Iterations since the last cede. */
  var n: Int = 0
  var i0: Int = 0
  var i1: Int = 0
  var i2: Int = 0
  var i3: Int = 0
  var l0: Long = 0L
  var l1: Long = 0L
  var a0: AnyRef = null
  var a1: AnyRef = null
  var a2: AnyRef = null
  var a3: AnyRef = null
  val is: Array[Int] = if ints > 4 then new Array[Int](ints - 4) else null
  val ls: Array[Long] = if longs > 2 then new Array[Long](longs - 2) else null
  val as: Array[AnyRef] = if refs > 4 then new Array[AnyRef](refs - 4) else null
  def apply(v: Any): AnyRef = body(this, v)
