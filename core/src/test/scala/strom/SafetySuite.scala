package strom

import strom.runtime.Segment.Status

class SafetySuite extends munit.FunSuite:
  test("an exception poisons the segment") {
    val seg = Strom.segment((a: Strom[Int]) => a.map(x => 100 / x)).create(maxOut = 8, budget = 100)
    assertEquals(seg.step(), 0)
    seg.offer(0, Array(5, 0, 2), 0, 3)
    intercept[ArithmeticException](seg.step())
    assert(seg.isPoisoned)
    val e = intercept[IllegalStateException](seg.step())
    assert(e.getCause.isInstanceOf[ArithmeticException])
  }

  test("bounded work: a step does at most `budget` producer steps") {
    // flatMap expands each element into many inner elements, all filtered out.
    val seg = Strom.segment((a: Strom[Int]) => a.flatMap(x => Strom.range(0, x)).filter(_ < 0)).create(8, budget = 10)
    assertEquals(seg.step(), 0)
    seg.offer(0, Array(1_000_000), 0, 1)
    var yields = 0
    var st = seg.step()
    while st == Status.Yield do { yields += 1; st = seg.step() }
    assertEquals(st, 0)
    assert(yields >= 1_000_000 / 10 - 1, s"yields=$yields")
  }

  test("bounded output: a step emits at most maxOut elements") {
    val seg = Strom.segment((a: Strom[Int]) => a.flatMap(x => Strom.range(0, x))).create(maxOut = 3, budget = 1000)
    seg.step()
    seg.offer(0, Array(10), 0, 1)
    assertEquals(seg.step(), Status.Emit)
    assertEquals(seg.outLen, 3)
    assertEquals(seg.out.asInstanceOf[Array[Int]].toList, List(0, 1, 2))
  }

  test("take stops without asking for more input") {
    val seg = Strom.segment((a: Strom[Int]) => a.take(2)).create(8, 100)
    assertEquals(seg.step(), 0)
    seg.offer(0, Array(1, 2, 3), 0, 3)
    assertEquals(seg.step(), Status.Done)
    assertEquals(seg.outLen, 2)
  }

  test("output buffers are primitive arrays") {
    assert(Strom.segment((a: Strom[Int]) => a.map(_.toLong)).create().out.isInstanceOf[Array[Long]])
    assert(Strom.segment((a: Strom[Int]) => a.map(_.toDouble)).create().out.isInstanceOf[Array[Double]])
  }
