package strom.loom

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.util.Random
import strom.{Pipelines, Reference, Strom}
import strom.loom.StromLoom.*

class StromLoomSuite extends munit.FunSuite:
  def feed(xs: Array[Int], r: Random): ArrayBlockingQueue[Msg] =
    val q = ArrayBlockingQueue[Msg](4) // small: the producer blocks, exercising back-pressure
    Thread.ofVirtual().start { () =>
      var i = 0
      while i < xs.length do
        val n = math.min(xs.length - i, 1 + r.nextInt(7))
        q.put(Data(xs, i, n)); i += n
      q.put(End)
    }
    q

  def drain(q: ArrayBlockingQueue[Msg]): (List[Int], Option[Any]) =
    val out = List.newBuilder[Int]
    var result: Option[Any] = None
    var done = false
    while !done do
      q.poll(10, TimeUnit.SECONDS) match
        case Data(a, off, len) => for i <- off until off + len do out += a.asInstanceOf[Array[Int]](i)
        case Result(v) => result = Some(v)
        case End => done = true
        case Failed(e) => throw e
        case null => fail("timeout")
    (out.result(), result)

  test("two-input segment on a virtual thread agrees with the reference") {
    val f = Strom.segment(Pipelines.representativeEmit(30))
    val r = Random(9)
    for _ <- 0 until 50 do
      val a = Array.fill(r.nextInt(30))(r.nextInt(8)); val b = Array.fill(r.nextInt(60))(r.nextInt(9))
      val out = ArrayBlockingQueue[Msg](2)
      StromLoom.start(f, Vector(feed(a, Random(r.nextLong())), feed(b, Random(r.nextLong()))), out, maxOut = 1 + r.nextInt(4), budget = 1 + r.nextInt(9))
      assertEquals(drain(out)._1, Reference.toList(Pipelines.representativeEmit(30)(Strom.fromSeq(a.toSeq), Strom.fromSeq(b.toSeq))))
  }

  test("interrupting a busy segment stops it") {
    // An input whose expansion is effectively endless and filtered out: the segment only yields.
    val f = Strom.segment((a: Strom[Int]) => a.flatMap(x => Strom.range(0, x)).filter(_ < 0).count)
    val in = ArrayBlockingQueue[Msg](1)
    in.put(Data(Array(Int.MaxValue), 0, 1))
    val t = StromLoom.start(f, Vector(in), ArrayBlockingQueue[Msg](1), budget = 1000)
    Thread.sleep(50)
    t.interrupt()
    assert(t.join(java.time.Duration.ofSeconds(5)), "segment thread did not stop")
  }

  test("a failure is forwarded downstream") {
    val f = Strom.segment((a: Strom[Int]) => a.map(x => 10 / x))
    val in = ArrayBlockingQueue[Msg](2); in.put(Data(Array(1, 0), 0, 2)); in.put(End)
    val out = ArrayBlockingQueue[Msg](4)
    StromLoom.start(f, Vector(in), out)
    intercept[ArithmeticException](drain(out))
  }
