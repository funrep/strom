package strom

import scala.util.Random
import strom.runtime.*

class RunnerSuite extends munit.FunSuite:
  /** A minimal driver over in-memory inputs, as an adapter would write it. */
  def driveAll[O](r: Runner[O], inputs: Vector[Array[Int]], chunk: Int): (List[Any], Int) =
    val pos = Array.fill(inputs.size)(0)
    val out = List.newBuilder[Any]
    var yields = 0
    var done = false
    while !done do
      r.next() match
        case Runner.NeedInput(p) =>
          if pos(p) < inputs(p).length then
            val n = math.min(chunk, inputs(p).length - pos(p))
            r.offer(p, inputs(p), pos(p), n); pos(p) += n
          else r.endOfInput(p)
        case Runner.Emit =>
          val a = r.outArray.asInstanceOf[Array[Int]]
          for i <- r.outOffset until r.outOffset + r.outLength do out += a(i)
        case Runner.Yield => yields += 1
        case Runner.Done => done = true
    (out.result(), yields)

  test("runner drives an emitting segment") {
    val f = Strom.segment(Pipelines.representativeEmit(1000))
    val r = Random(3)
    for _ <- 0 until 200 do
      val a = Array.fill(r.nextInt(50))(r.nextInt(10)); val b = Array.fill(r.nextInt(80))(r.nextInt(10))
      val (got, _) = driveAll(Runner(f, maxOut = 1 + r.nextInt(5), budget = 1 + r.nextInt(9)), Vector(a, b), 1 + r.nextInt(7))
      assertEquals(got, Reference.toList(Pipelines.representativeEmit(1000)(Strom.fromSeq(a.toSeq), Strom.fromSeq(b.toSeq))))
  }

  test("runner reports Yield on budget exhaustion and the fold result on Done") {
    val f = Strom.segment((a: Strom[Int]) => a.flatMap(x => Strom.range(0, x)).filter(_ % 1000 == 0).count)
    val run = Runner(f, budget = 100, sliceNanos = 0) // yield at every budget boundary
    val (_, yields) = driveAll(run, Vector(Array(100_000)), 1)
    assertEquals(run.result, 100L)
    assert(yields >= 999)
  }

  test("with a time slice, yields are rare") {
    val f = Strom.segment((a: Strom[Int]) => a.flatMap(x => Strom.range(0, x)).filter(_ % 1000 == 0).count)
    val run = Runner(f, budget = 100, sliceNanos = 10_000_000_000L) // 10 s: effectively never
    val (_, yields) = driveAll(run, Vector(Array(100_000)), 1)
    assertEquals(run.result, 100L)
    assertEquals(yields, 0)
  }

  test("NeedInput signals are preallocated") {
    assert(Runner.needInput(1) eq Runner.needInput(1))
  }

  test("key partitioner is stable and spreads consecutive keys") {
    val counts = Array.fill(8)(0)
    for k <- 0 until 80_000 do counts(KeyPartitioner.partition(k, 8)) += 1
    assert(counts.forall(c => c > 9000 && c < 11000), counts.toList)
    assertEquals(KeyPartitioner.partitionOf("abc", 5), KeyPartitioner.partitionOf("abc", 5))
  }

  test("reorder buffer releases in sequence order") {
    val rb = ReorderBuffer[String](4)
    rb.put(2, "c"); rb.put(1, "b")
    assertEquals(rb.poll(), null)
    rb.put(0, "a")
    assertEquals(List(rb.poll(), rb.poll(), rb.poll(), rb.poll()), List("a", "b", "c", null))
    assert(!rb.accepts(7)); assert(rb.accepts(6))
    intercept[IllegalArgumentException](rb.put(7, "x"))
  }

  test("in-flight accounting") {
    val f = InFlight(3)
    assert(f.tryAcquire(2)); assert(!f.tryAcquire(2)); assert(f.tryAcquire())
    f.release(3); assertEquals(f.inFlight, 0)
    intercept[IllegalStateException](f.release())
  }
