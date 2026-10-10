package strom.loom

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.util.Random
import strom.{Fold, Reference, Strom}
import strom.loom.StromLoom.*
import strom.runtime.{KeyPartitioner, ParallelPlan}

class LoomParallelSuite extends munit.FunSuite:
  def feed(xs: Array[Int], r: Random, q: ArrayBlockingQueue[Msg]): Unit =
    Thread.ofVirtual().start { () =>
      var i = 0
      while i < xs.length do
        val n = math.min(xs.length - i, 1 + r.nextInt(20))
        q.put(Data(xs, i, n)); i += n
      q.put(End)
    }

  def runPlan(plan: ParallelPlan[?], xs: Array[Int], cfg: LoomParallel.Config, r: Random): (List[Any], Option[Any]) =
    val in = ArrayBlockingQueue[Msg](4)
    val out = ArrayBlockingQueue[Msg](4)
    val running = LoomParallel.start(plan, in, out, cfg)
    feed(xs, Random(r.nextLong()), in)
    val elems = List.newBuilder[Any]
    var result: Option[Any] = None
    var done = false
    while !done do
      out.poll(20, TimeUnit.SECONDS) match
        case Data(a, off, len) => for i <- off until off + len do elems += java.lang.reflect.Array.get(a, i)
        case Result(v) => result = Some(v)
        case End => done = true
        case Failed(e) => throw e
        case null => running.cancel(); fail("timeout")
    running.join()
    (elems.result(), result)

  def configs(r: Random) = LoomParallel.Config(
    parallelism = 1 + r.nextInt(6), queueCapacity = 1 + r.nextInt(4), maxOut = 1 + r.nextInt(64), budget = 1 + r.nextInt(500),
    compute = if r.nextBoolean() then Thread.ofPlatform().daemon().factory() else Thread.ofVirtual().factory())

  inline def stateless = (a: Strom[Int]) => a.map(_ * 3).filter(_ % 2 == 0).flatMap(x => Strom.range(0, x % 6))
  inline def cut = (a: Strom[Int]) => a.flatMap(x => Strom.range(0, x % 5)).filter(_ != 2).take(300).map(_ + 1)
  inline def partial = (a: Strom[Int]) => a.map(x => x * x).filter(_ % 3 != 0).sum
  inline def nonAssoc = (a: Strom[Int]) => a.map(_ * 2).fold(7)((s, x) => s * 31 + x)
  inline def windowed = (a: Strom[Int]) => a.windowFold(2)(0)(_ * 10 + _)

  def randomInput(r: Random) = Array.fill(r.nextInt(2000))(r.nextInt(50) - 5)

  test("replicated tier keeps input order") {
    val plan = Strom.parallel(stateless); val r = Random(1)
    for _ <- 0 until 60 do
      val xs = randomInput(r)
      assertEquals(runPlan(plan, xs, configs(r), r)._1, Reference.toList(stateless(Strom.fromSeq(xs.toSeq))))
  }

  test("replicated prefix + single tier") {
    val plan = Strom.parallel(cut); val r = Random(2)
    for _ <- 0 until 60 do
      val xs = randomInput(r)
      assertEquals(runPlan(plan, xs, configs(r), r)._1, Reference.toList(cut(Strom.fromSeq(xs.toSeq))))
  }

  test("partial folds combine in order") {
    val plan = Strom.parallelFold(partial); val r = Random(3)
    for _ <- 0 until 60 do
      val xs = randomInput(r)
      assertEquals(runPlan(plan, xs, configs(r), r)._2, Some(Reference.run(partial(Strom.fromSeq(xs.toSeq)))))
  }

  test("non-associative fold runs as a single tier after the replicated prefix") {
    val plan = Strom.parallelFold(nonAssoc); val r = Random(4)
    for _ <- 0 until 60 do
      val xs = randomInput(r)
      assertEquals(runPlan(plan, xs, configs(r), r)._2, Some(Reference.run(nonAssoc(Strom.fromSeq(xs.toSeq)))))
  }

  test("keyed tier: per-partition state") {
    val plan = Strom.parallelKeyed((x: Int) => x % 11)(windowed); val r = Random(5)
    for _ <- 0 until 60 do
      val xs = randomInput(r)
      val cfg = configs(r)
      val expected = (0 until cfg.parallelism).flatMap(p =>
        Reference.toList(windowed(Strom.fromSeq(xs.toSeq.filter(x => KeyPartitioner.partition(x % 11, cfg.parallelism) == p)))))
      assertEquals(runPlan(plan, xs, cfg, r)._1.map(_.asInstanceOf[Int]).sorted, expected.toList.sorted)
  }

  test("a worker failure is reported downstream") {
    val plan = Strom.parallel((a: Strom[Int]) => a.map(x => 100 / x))
    intercept[ArithmeticException](runPlan(plan, Array.fill(500)(1) :+ 0, LoomParallel.Config(parallelism = 3), Random(6)))
  }

  test("empty input") {
    assertEquals(runPlan(Strom.parallelFold(partial), Array.empty, LoomParallel.Config(parallelism = 2), Random(7))._2, Some(0))
    assertEquals(runPlan(Strom.parallel(stateless), Array.empty, LoomParallel.Config(parallelism = 2), Random(7))._1, Nil)
  }
