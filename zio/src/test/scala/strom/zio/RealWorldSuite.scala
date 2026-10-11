package strom.zio

import _root_.zio.{Chunk, Exit, Ref, Runtime, Unsafe, ZIO, durationInt}
import _root_.zio.stream.ZStream

/** Fused ZStream code next to real effects: effect order, failures and defects, resources,
 *  interruption, timeouts, early termination and concurrency. Each case compares the fused stream
 *  with the same plain ZStream code. */
class RealWorldSuite extends munit.FunSuite:
  final class Boom(msg: String) extends RuntimeException(msg)

  def run[E, A](z: ZIO[Any, E, A]): A =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z.timeoutFail(new RuntimeException("test timeout"))(20.seconds)).getOrThrowFiberFailure())
  def exit[E, A](z: ZIO[Any, E, A]): Exit[E, A] =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z))

  def logged[E, A](mk: Ref[Vector[String]] => ZStream[Any, E, A]): (List[A], Vector[String]) =
    run(for
      log <- Ref.make(Vector.empty[String])
      out <- mk(log).runCollect
      l   <- log.get
    yield (out.toList, l))

  def chunked(n: Int, size: Int): ZStream[Any, Nothing, Int] =
    ZStream.fromChunks((0 until n).grouped(size).map(c => Chunk.fromIterable(c)).toSeq*)

  test("postfix syntax: .fused / .fusedPar keep the stream type") {
    val a: ZStream[Any, Nothing, Int] = chunked(500, 7).map(_ * 3).filter(_ % 2 == 0).mapZIO(ZIO.succeed(_)).map(_ + 1).fused
    val b: ZStream[Any, Nothing, Int] = chunked(500, 7).map(_ * 3).filter(_ % 2 == 0).mapZIO(ZIO.succeed(_)).map(_ + 1).fusedPar(3)
    val plain = chunked(500, 7).map(_ * 3).filter(_ % 2 == 0).mapZIO(ZIO.succeed(_)).map(_ + 1)
    assertEquals(run(a.runCollect), run(plain.runCollect))
    assertEquals(run(b.runCollect), run(plain.runCollect))
  }

  test("effects between fused runs: same output, same output-side effects") {
    def p(log: Ref[Vector[String]]) =
      chunked(300, 11).tap(x => log.update(_ :+ s"in $x")).map(_ * 7).filter(_ % 3 != 0)
        .mapZIO(x => log.update(_ :+ s"mid $x").as(x + 1)).map(_ * 2).filter(_ % 5 != 0).take(60)
        .tap(x => log.update(_ :+ s"out $x"))
    def f(log: Ref[Vector[String]]) = StromZio.fused(
      chunked(300, 11).tap(x => log.update(_ :+ s"in $x")).map(_ * 7).filter(_ % 3 != 0)
        .mapZIO(x => log.update(_ :+ s"mid $x").as(x + 1)).map(_ * 2).filter(_ % 5 != 0).take(60)
        .tap(x => log.update(_ :+ s"out $x")))
    val (po, pl) = logged(p)
    val (fo, fl) = logged(f)
    assertEquals(fo, po)
    assertEquals(fl.filter(_.startsWith("out")), pl.filter(_.startsWith("out")))
    assert(pl.filter(_.startsWith("in")).toSet.subsetOf(fl.filter(_.startsWith("in")).toSet))
  }

  test("take on an infinite effectful source stops") {
    def f(log: Ref[Vector[String]]) = StromZio.fused(
      ZStream.iterate(0)(_ + 1).tap(x => log.update(_ :+ s"$x")).map(_ * 3).filter(_ % 2 == 0).take(25))
    def p(log: Ref[Vector[String]]) =
      ZStream.iterate(0)(_ + 1).tap(x => log.update(_ :+ s"$x")).map(_ * 3).filter(_ % 2 == 0).take(25)
    assertEquals(logged(f)._1, logged(p)._1)
  }

  test("a throwing lambda is a defect, as in plain ZStream; typed failures pass through") {
    val f = StromZio.fused(chunked(1000, 13).map(x => if x == 437 then throw Boom("bad") else x * 2).filter(_ % 3 != 0))
    val p = chunked(1000, 13).map(x => if x == 437 then throw Boom("bad") else x * 2).filter(_ % 3 != 0)
    def dieMsg(e: Exit[Nothing, ?]) = e.causeOption.flatMap(_.dieOption).map(_.getMessage)
    assertEquals(dieMsg(exit(f.runCollect)), Some("bad"))
    assertEquals(dieMsg(exit(p.runCollect)), Some("bad"))

    val typed = (chunked(200, 9) ++ ZStream.fail("upstream"))
    val fs = StromZio.fused(typed.map(_ + 1).filter(_ % 2 == 0).catchAll(_ => ZStream(-1)))
    val ps = typed.map(_ + 1).filter(_ % 2 == 0).catchAll(_ => ZStream(-1))
    assertEquals(run(fs.runCollect), run(ps.runCollect))
  }

  test("finalizers run once, in order, also when take stops early or a lambda throws") {
    def f(log: Ref[Vector[String]]) = StromZio.fused(
      ZStream.acquireReleaseWith(log.update(_ :+ "acq"))(_ => log.update(_ :+ "rel"))
        .flatMap(_ => chunked(10000, 50)).map(_ * 2).filter(_ % 3 == 0).take(40).ensuring(log.update(_ :+ "fin")))
    def p(log: Ref[Vector[String]]) =
      ZStream.acquireReleaseWith(log.update(_ :+ "acq"))(_ => log.update(_ :+ "rel"))
        .flatMap(_ => chunked(10000, 50)).map(_ * 2).filter(_ % 3 == 0).take(40).ensuring(log.update(_ :+ "fin"))
    val log0 = run(Ref.make(Vector.empty[String]))
    assertEquals(StromZio.fusedPlan(ZStream.acquireReleaseWith(log0.update(_ :+ "acq"))(_ => log0.update(_ :+ "rel"))
        .flatMap(_ => chunked(10000, 50)).map(_ * 2).filter(_ % 3 == 0).take(40).ensuring(log0.update(_ :+ "fin"))),
      "fused: map → filter → take (1 inputs)\nkept: flatMap, ensuring")
    assertEquals(logged(f), logged(p))
    assertEquals(logged(f)._2, Vector("acq", "rel", "fin"))

    val fin = run(for
      log <- Ref.make(Vector.empty[String])
      _   <- StromZio.fused(chunked(500, 7).map(x => if x == 250 then throw Boom("x") else x).filter(_ > 0))
               .ensuring(log.update(_ :+ "fin")).runDrain.catchAllDefect(_ => ZIO.unit)
      l   <- log.get
    yield l)
    assertEquals(fin, Vector("fin"))
  }

  test("endless fused loops are interruptible: interruptAfter, timeout, also with no output") {
    val n = run(StromZio.fused(ZStream.iterate(0)(_ + 1).map(x => x * 31 + 7).filter(_ % 3 != 0).map(_ ^ 5))
      .interruptAfter(200.millis).runCount)
    assert(n > 0)
    val none = run(StromZio.fused(ZStream.iterate(0)(_ + 1).map(_ * 2).filter(_ < 0)).interruptAfter(200.millis).runCount)
    assertEquals(none, 0L)
    val t = run(StromZio.fused(ZStream.iterate(0)(_ + 1).map(_ * 2).filter(_ < 0)).timeout(150.millis).runCount)
    assertEquals(t, 0L)
    val par = run(StromZio.fusedPar(4)(ZStream.iterate(0)(_ + 1).map(x => x * 31 + 7).filter(_ % 3 != 0))
      .interruptAfter(200.millis).runCount)
    assert(par > 0)
  }

  test("fused inner streams under flatMapPar / mapZIOPar / merge, and one value run concurrently") {
    def inner(i: Int) = StromZio.fused(chunked(200 + i, 1 + i % 9).map(_ * i).filter(_ % 3 != 0).map(_ + 1))
    def innerPlain(i: Int) = chunked(200 + i, 1 + i % 9).map(_ * i).filter(_ % 3 != 0).map(_ + 1)
    val joined = run(ZStream.range(0, 30).flatMapPar(8)(inner).runCollect).toList
    val joinedPlain = run(ZStream.range(0, 30).flatMapPar(8)(innerPlain).runCollect).toList
    assertEquals(joined.sorted, joinedPlain.sorted)

    val sums = run(ZStream.range(0, 30).mapZIOPar(8)(i => inner(i).runSum).runCollect).toList
    assertEquals(sums, (0 until 30).toList.map(i => run(innerPlain(i).runSum)))

    assertEquals(run(inner(3).merge(inner(5)).runCollect).toList.sorted,
      (run(innerPlain(3).runCollect) ++ run(innerPlain(5).runCollect)).toList.sorted)

    val s = StromZio.fused(chunked(3000, 17).map(_ * 3).filter(_ % 2 == 0).scan(0)(_ + _).take(2000))
    val expected = run(chunked(3000, 17).map(_ * 3).filter(_ % 2 == 0).scan(0)(_ + _).take(2000).runCollect)
    val results = run(ZIO.foreachPar(1 to 64)(_ => s.runCollect))
    assert(results.forall(_ == expected))
  }

  test("fusedPar: finalizers and errors under real effects") {
    def f(log: Ref[Vector[String]]) = StromZio.fusedPar(4)(
      chunked(2000, 13).mapZIO(ZIO.succeed(_)).map(x => x * 7).filter(_ % 3 != 0).map(_ + 1).ensuring(log.update(_ :+ "fin")))
    def p(log: Ref[Vector[String]]) =
      chunked(2000, 13).mapZIO(ZIO.succeed(_)).map(x => x * 7).filter(_ % 3 != 0).map(_ + 1).ensuring(log.update(_ :+ "fin"))
    assertEquals(logged(f), logged(p))
    val e = exit(StromZio.fusedPar(4)(chunked(2000, 13).map(x => if x == 1500 then throw Boom("par") else x).filter(_ > 0)).runDrain)
    assertEquals(e.causeOption.flatMap(_.dieOption).map(_.getMessage), Some("par"))
  }
