package strom

import java.lang.management.ManagementFactory
import strom.runtime.Segment.Status

/** The fused path allocates nothing per element. Measured with the JVM's per-thread allocation
 *  counter over a million elements; whatever is allocated must not scale with the input. */
class AllocationSuite extends munit.FunSuite:
  private val mx = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
  private def allocated(body: => Unit): Long =
    val t = Thread.currentThread().threadId()
    val before = mx.getThreadAllocatedBytes(t)
    body
    mx.getThreadAllocatedBytes(t) - before

  val a = Array.tabulate(1 << 20)(i => i % 13)
  val b = Array.tabulate(1 << 20)(i => i % 7)

  test("segment: no per-element allocation") {
    val f = Strom.foldSegment2(Pipelines.representative(Int.MaxValue))
    def runOnce(): Any =
      val seg = f.create(maxOut = 256, budget = 4096)
      var pa = 0; var pb = 0; var st = seg.step()
      while st != Status.Done do
        st match
          case 0 => if pa < a.length then { seg.offer(0, a, pa, math.min(4096, a.length - pa)); pa += 4096 } else seg.endOfInput(0)
          case 1 => if pb < b.length then { seg.offer(1, b, pb, math.min(1024, b.length - pb)); pb += 1024 } else seg.endOfInput(1)
          case _ => ()
        st = seg.step()
      seg.result
    for _ <- 0 until 5 do runOnce() // warm up
    val bytes = allocated(runOnce())
    // One segment instance (slots + ports), one boxed result: well under 4 KB, vs > 8 MB for 1M boxes.
    assert(bytes < 4096, s"allocated $bytes bytes for ${a.length + b.length} input elements")
  }

  test("direct: no allocation at all") {
    def runOnce(): Int =
      Strom.run(Strom.fromArray(a).flatMap(x => Strom.range(0, x % 3)).zipWith(Strom.fromArray(b).filter(_ % 2 == 0))(_ * _).sum)
    for _ <- 0 until 5 do runOnce()
    var r = 0
    val bytes = allocated { r = runOnce() }
    assert(bytes < 256, s"allocated $bytes bytes")
  }
