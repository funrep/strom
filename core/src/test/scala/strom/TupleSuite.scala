package strom

import java.lang.management.ManagementFactory

/** Tuples built by zip / zipWithIndex / user code stay unboxed when only projected or matched. */
class TupleSuite extends munit.FunSuite:
  val (a, b) = (Array.tabulate(1000)(i => i % 17), Array.tabulate(1000)(i => i % 5))

  test("zipWithIndex + untupled lambda: no tuple in the generated code") {
    val code = Strom.showCode((s: Strom[Int]) => s.zipWithIndex.map((x, i) => x + i.toInt).filter(_ % 2 == 0))
    assert(!code.contains("Tuple2"), code)
  }

  test("zip + collect on a tuple pattern with a guard: no tuple") {
    val code = Strom.showCode((s: Strom[Int], t: Strom[Int]) => s.zip(t).collect { case (x, y) if x < y => x - y })
    assert(!code.contains("Tuple2"), code)
  }

  test("user tuples and projections: no tuple") {
    val code = Strom.showCode(Strom.fromArray(a).map(x => (x, x * 2)).map(_._2).filter(_ > 3).sum)
    assert(!code.contains("Tuple2"), code)
  }

  test("mapAccumFilter: no tuple / Option allocated for literal results") {
    val code = Strom.showCode((s: Strom[Int]) =>
      s.mapAccumFilter(0)((acc, x) => if x > 3 then (acc + x, Some(acc)) else (acc, None)))
    assert(!code.contains("Tuple2") && !code.contains("Some"), code)
  }

  test("a tuple that escapes is still built") {
    val code = Strom.showCode((s: Strom[Int]) => s.zipWithIndex)
    assert(code.contains("Tuple2"), code)
  }

  test("results agree with the reference") {
    assertEquals(Strom.run(Strom.fromArray(a).zipWithIndex.map((x, i) => x * i.toInt).sum),
      Reference.run(Strom.fromArray(a).zipWithIndex.map((x, i) => x * i.toInt).sum))
    assertEquals(Strom.run(Strom.fromArray(a).zip(Strom.fromArray(b)).collect { case (x, y) if x > y => x - y; case (_, y) => y }.sum),
      Reference.run(Strom.fromArray(a).zip(Strom.fromArray(b)).collect { case (x, y) if x > y => x - y; case (_, y) => y }.sum))
    assertEquals(Strom.run(Strom.fromArray(a).map(x => (x, x * 2)).map(t => t._1 + t._2).sum),
      Reference.run(Strom.fromArray(a).map(x => (x, x * 2)).map(t => t._1 + t._2).sum))
  }

  test("zipWithIndex allocates nothing per element") {
    val big = Array.tabulate(1 << 20)(i => i % 13)
    def once() = Strom.run(Strom.fromArray(big).zipWithIndex.collect { case (x, i) if i % 3 != 0 => x * 2 }.sum)
    for _ <- 0 until 5 do once()
    val mx = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    val t = Thread.currentThread().threadId()
    val before = mx.getThreadAllocatedBytes(t)
    once()
    val bytes = mx.getThreadAllocatedBytes(t) - before
    assert(bytes < 1024, s"allocated $bytes bytes for ${big.length} elements")
  }
