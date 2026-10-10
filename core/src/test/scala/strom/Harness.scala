package strom

import scala.reflect.ClassTag
import scala.util.Random
import strom.runtime.{Segment, SegmentFactory}
import strom.runtime.Segment.Status

/** Drives a segment over in-memory inputs with a random chunking, random output capacity,
 *  random work budget and random choice of empty/short/long chunks. Checks the protocol. */
object Harness:
  final case class Config(seed: Long, maxOut: Int, budget: Int, maxChunk: Int)

  def randomConfig(r: Random): Config =
    Config(r.nextLong(), 1 + r.nextInt(if r.nextBoolean() then 4 else 64), 1 + r.nextInt(if r.nextBoolean() then 3 else 200),
      1 + r.nextInt(if r.nextBoolean() then 3 else 50))

  final case class Outcome(outputs: Vector[Any], result: Option[Any], steps: Int)

  def drive(f: SegmentFactory[?], inputs: Seq[Array[?]], cfg: Config): Outcome =
    val r = Random(cfg.seed)
    val seg = f.create(cfg.maxOut, cfg.budget)
    val pos = Array.fill(inputs.size)(0)
    val out = Vector.newBuilder[Any]
    var steps = 0
    var done = false
    while !done do
      steps += 1
      if steps > 10_000_000 then throw AssertionError("segment does not make progress")
      val st = seg.step()
      assert(seg.outLen >= 0 && seg.outLen <= cfg.maxOut, s"outLen ${seg.outLen}")
      if seg.outLen > 0 then
        assert(!f.isFold, "fold segment emitted output")
        val arr = seg.out
        var i = 0
        while i < seg.outLen do
          out += java.lang.reflect.Array.get(arr, i)
          i += 1
      st match
        case Status.Emit  => assert(seg.outLen == cfg.maxOut, "Emit with non-full buffer")
        case Status.Yield => ()
        case Status.Done  => done = true
        case p if p >= 0 && p < inputs.size =>
          assert(seg.remaining(p) == 0, s"NeedInput($p) with ${seg.remaining(p)} buffered")
          assert(!seg.isEnded(p), s"NeedInput($p) after end of input")
          val in = inputs(p)
          val len = java.lang.reflect.Array.getLength(in)
          if pos(p) >= len then seg.endOfInput(p)
          else
            // Occasionally offer an empty chunk: the segment must just ask again.
            val n = if r.nextInt(10) == 0 then 0 else math.min(len - pos(p), 1 + r.nextInt(cfg.maxChunk))
            // Copy into a larger array at a random offset, to exercise offsets.
            val off = r.nextInt(3)
            val buf = java.lang.reflect.Array.newInstance(in.getClass.getComponentType, off + n + r.nextInt(3))
            System.arraycopy(in, pos(p), buf, off, n)
            seg.offer(p, buf, off, n)
            pos(p) += n
        case other => throw AssertionError(s"unexpected status $other")
    // After Done, the segment stays done and emits nothing.
    assertEquals(seg.step(), Status.Done)
    assertEquals(seg.outLen, 0)
    Outcome(out.result(), if f.isFold then Some(seg.result) else None, steps)

  private def assertEquals(a: Any, b: Any): Unit = assert(a == b, s"$a != $b")
