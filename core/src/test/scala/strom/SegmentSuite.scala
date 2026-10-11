package strom

import scala.util.Random
import strom.runtime.SegmentFactory

/** Every staged segment agrees with the reference interpreter, for random inputs, random
 *  chunkings, random output capacities and random work budgets. */
class SegmentSuite extends munit.FunSuite:
  import Pipelines.*

  val Iterations = 400

  def randomInts(r: Random): Array[Int] =
    Array.fill(r.nextInt(if r.nextInt(5) == 0 then 200 else 25))(r.nextInt(70) - 20)

  def check1[O](name: String, f: SegmentFactory[O], ref: Strom[Int] => Strom[O] | Fold[O]): Unit =
    test(name) {
      val r = Random(name.hashCode)
      for i <- 0 until Iterations do
        val xs = randomInts(r)
        val cfg = Harness.randomConfig(r)
        val got = Harness.drive(f, Seq(xs), cfg)
        compare(got, ref(Strom.fromSeq(xs.toSeq)), s"iteration $i, xs=${xs.mkString(",")}, $cfg")
    }

  def check2[O](name: String, f: SegmentFactory[O], ref: (Strom[Int], Strom[Int]) => Strom[O] | Fold[O]): Unit =
    test(name) {
      val r = Random(name.hashCode)
      for i <- 0 until Iterations do
        val xs = randomInts(r)
        val ys = randomInts(r)
        val cfg = Harness.randomConfig(r)
        val got = Harness.drive(f, Seq(xs, ys), cfg)
        compare(got, ref(Strom.fromSeq(xs.toSeq), Strom.fromSeq(ys.toSeq)),
          s"iteration $i, xs=${xs.mkString(",")}, ys=${ys.mkString(",")}, $cfg")
    }

  def compare(got: Harness.Outcome, expected: Strom[?] | Fold[?], ctx: => String): Unit = expected match
    case s: Strom[?] => assertEquals(got.outputs.toList, Reference.toList(s), ctx)
    case f: Fold[?]  => assertEquals(got.result, Some(Reference.run(f)), ctx)

  val n = 9
  check2("representative (fold)", Strom.foldSegment2(representative(n)), representative(n))
  check2("representative (emit)", Strom.segment2(representativeEmit(n)), representativeEmit(n))
  check1("map/filter", Strom.segment(mapFilter), mapFilter)
  check1("take inside flatMap", Strom.segment(takeInside), takeInside)
  check1("take before flatMap", Strom.segment(takeBefore), takeBefore)
  check1("take after nested", Strom.segment(takeAfterNested(n)), takeAfterNested(n))
  check2("zip two ports", Strom.segment2(zipPorts), zipPorts)
  check2("zip two filtered ports", Strom.segment2(zipFilteredPorts), zipFilteredPorts)
  check1("zip port with range", Strom.segment(zipRange), zipRange)
  check1("zip range with filtered port", Strom.segment(zipRangeLeft), zipRangeLeft)
  check1("windowFold", Strom.segment(window(3)), window(3))
  check2("windowFold of nested, zipped", Strom.segment2(windowNestedZip), windowNestedZip)
  check1("zip inside flatMap", Strom.segment(zipInsideFlatMap), zipInsideFlatMap)
  check1("Long count", Strom.foldSegment(longCount), longCount)
  check1("Long sum", Strom.foldSegment(longSum), longSum)
  check1("Double sum", Strom.foldSegment(doubleSum), doubleSum)
  check1("String elements", Strom.segment(strings), strings)
  check2("zip to tuples", Strom.segment2(tuples), tuples)
  check1("three nesting levels", Strom.segment(threeLevels), threeLevels)
  check1("closure inside a lambda", Strom.segment(closureInLambda), closureInLambda)
  check1("take(0)", Strom.segment(takeZero), takeZero)
  check1("take(-3)", Strom.segment(takeNegative), takeNegative)
  check1("non-associative fold", Strom.foldSegment(nonAssoc), nonAssoc)
  check1("fold to tuple", Strom.foldSegment(foldTuple), foldTuple)
  check1("windowFold(0) acts as 1", Strom.segment(windowZero), windowZero)
  check2("take on one zip operand", Strom.segment2(takeOneSide(n)), takeOneSide(n))
  val arr = Array(1, 2, 3, 4, 5)
  check1("flatMap over captured array", Strom.segment(flatMapArray(arr)), flatMapArray(arr))
  check1("Byte elements", Strom.segment(bytes), bytes)

  check1("collect", Strom.segment(collected), collected)
  check2("collect on tuples", Strom.segment2(collectTuple), collectTuple)
  check1("mapFilter", Strom.segment(optional), optional)
  check1("zipWithIndex", Strom.segment(indexed), indexed)
  check1("scan + drop", Strom.segment(scanned), scanned)
  check1("dropWhile + takeWhile", Strom.segment(whiles), whiles)
  check1("new operators inside flatMap", Strom.segment(nestedNew), nestedNew)
  check2("scan zipped with indexed drop", Strom.segment2(zipScan), zipScan)

  check1("mapAccumFilter (run lengths)", Strom.segment(runLengths), runLengths)
  check1("mapAccum", Strom.segment(runningMax), runningMax)
  check1("mapAccum inside flatMap", Strom.segment(accumInFlatMap), accumInFlatMap)

  test("plan metadata") {
    val f = Strom.foldSegment2(representative(3))
    assert(!f.plan.stateless)
    assert(f.plan.associativeFold)
    assert(Strom.segment(mapFilter).plan.stateless)
    assert(!Strom.foldSegment(nonAssoc).plan.associativeFold)
  }
