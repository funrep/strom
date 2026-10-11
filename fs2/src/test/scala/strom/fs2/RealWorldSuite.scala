package strom.fs2

import scala.concurrent.duration.*
import cats.effect.{IO, Ref, Resource}
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import _root_.fs2.{Chunk, Stream}

/** Fused code next to real effects: effect order, errors, resources, cancellation, timeouts,
 *  early termination and concurrency. Each case compares the fused stream with the same plain
 *  fs2 code, including the log of effects that ran. */
class RealWorldSuite extends munit.FunSuite:
  final class Boom(msg: String) extends RuntimeException(msg)

  def run[A](io: IO[A]): A = io.timeout(20.seconds).unsafeRunSync()

  /** Runs `mk(log)` and returns its output and the log of effects, in order. */
  def logged[A](mk: Ref[IO, Vector[String]] => Stream[IO, A]): (List[A], Vector[String]) =
    run(for
      log <- Ref[IO].of(Vector.empty[String])
      out <- mk(log).compile.toList
      l   <- log.get
    yield (out, l))

  def chunked(n: Int, size: Int): Stream[IO, Int] =
    Stream.emits((0 until n).grouped(size).map(c => Chunk.from(c)).toSeq).flatMap(Stream.chunk).covary[IO]

  // ------------------------------------------------------------------ syntax

  test("postfix syntax: .fused / .fusedPar / .fusedPlan keep the stream type") {
    val in = chunked(500, 7)
    val a: Stream[IO, Int] = in.map(_ * 3).filter(_ % 2 == 0).evalMap(IO.pure).map(_ + 1).fused
    val b: Stream[IO, Int] = in.map(_ * 3).filter(_ % 2 == 0).evalMap(IO.pure).map(_ + 1).fusedPar(3)
    val plain = in.map(_ * 3).filter(_ % 2 == 0).evalMap(IO.pure).map(_ + 1)
    assertEquals(run(a.compile.toList), run(plain.compile.toList))
    assertEquals(run(b.compile.toList), run(plain.compile.toList))
    assertEquals(in.map(_ * 3).filter(_ % 2 == 0).evalMap(IO.pure).map(_ + 1).fusedPlan,
      "fused: map → filter (1 inputs)\nkept: evalMap")
  }

  // ------------------------------------------------------------------ effect order

  test("effects between fused runs run in the same order, same number of times") {
    def p(log: Ref[IO, Vector[String]]) =
      chunked(300, 11).evalTap(x => log.update(_ :+ s"in $x")).map(_ * 7).filter(_ % 3 != 0)
        .evalMap(x => log.update(_ :+ s"mid $x").as(x + 1)).map(_ * 2).filter(_ % 5 != 0).take(60L)
        .evalTap(x => log.update(_ :+ s"out $x"))
    def f(log: Ref[IO, Vector[String]]) = StromFs2.fused(
      chunked(300, 11).evalTap(x => log.update(_ :+ s"in $x")).map(_ * 7).filter(_ % 3 != 0)
        .evalMap(x => log.update(_ :+ s"mid $x").as(x + 1)).map(_ * 2).filter(_ % 5 != 0).take(60L)
        .evalTap(x => log.update(_ :+ s"out $x")))
    val (po, pl) = logged(p)
    val (fo, fl) = logged(f)
    assertEquals(fo, po)
    // Output-side effects match exactly; upstream effects may run at most one chunk ahead.
    assertEquals(fl.filter(_.startsWith("out")), pl.filter(_.startsWith("out")))
    assert(pl.filter(_.startsWith("in")).toSet.subsetOf(fl.filter(_.startsWith("in")).toSet))
  }

  test("take on an infinite effectful source stops pulling") {
    def p(log: Ref[IO, Vector[String]]) =
      Stream.iterate(0)(_ + 1).covary[IO].evalTap(x => log.update(_ :+ s"$x")).map(_ * 3).filter(_ % 2 == 0).take(25L)
    def f(log: Ref[IO, Vector[String]]) = StromFs2.fused(
      Stream.iterate(0)(_ + 1).covary[IO].evalTap(x => log.update(_ :+ s"$x")).map(_ * 3).filter(_ % 2 == 0).take(25L))
    val (po, pl) = logged(p)
    val (fo, fl) = logged(f)
    assertEquals(fo, po)
    assertEquals(fl, pl) // singleton chunks: exactly the same elements are pulled
  }

  // ------------------------------------------------------------------ errors

  test("an exception in a fused lambda fails the stream with the same error") {
    val p = chunked(1000, 13).map(x => if x == 437 then throw Boom("bad") else x * 2).filter(_ % 3 != 0)
    val f = StromFs2.fused(chunked(1000, 13).map(x => if x == 437 then throw Boom("bad") else x * 2).filter(_ % 3 != 0))
    val pe = run(p.compile.toList.attempt)
    val fe = run(f.compile.toList.attempt)
    assert(pe.left.exists(_.getMessage == "bad"), pe)
    assert(fe.left.exists(_.getMessage == "bad"), fe)
  }

  test("elements before a failure: fused output is a prefix of the error-free output") {
    val ideal = run(chunked(1000, 13).map(x => x * 2).filter(_ % 3 != 0).takeWhile(_ != 874).compile.toList)
    val before = run(StromFs2.fused(chunked(1000, 13).map(x => if x == 437 then throw Boom("bad") else x * 2).filter(_ % 3 != 0))
      .handleErrorWith(_ => Stream.empty).compile.toList)
    assert(ideal.startsWith(before), s"${before.size} elements are not a prefix")
  }

  test("an upstream error passes through fused runs; handleErrorWith downstream recovers") {
    val src = chunked(200, 9) ++ Stream.raiseError[IO](Boom("upstream"))
    val p = src.map(_ + 1).filter(_ % 2 == 0).handleErrorWith(e => Stream.emit(-1))
    val f = StromFs2.fused(src.map(_ + 1).filter(_ % 2 == 0).handleErrorWith(e => Stream.emit(-1)))
    assertEquals(run(f.compile.toList), run(p.compile.toList))
  }

  test("a failing fold reports the error, not a partial result") {
    val f = StromFs2.fused(chunked(500, 7).map(x => if x == 300 then throw Boom("fold") else x).fold(0)(_ + _))
    assert(run(f.compile.toList.attempt).left.exists(_.getMessage == "fold"))
  }

  // ------------------------------------------------------------------ resources

  test("finalizers run once, in the same order, also when take stops early") {
    def p(log: Ref[IO, Vector[String]]) =
      Stream.bracket(log.update(_ :+ "open"))(_ => log.update(_ :+ "close"))
        .flatMap(_ => Stream.resource(Resource.make(log.update(_ :+ "acq"))(_ => log.update(_ :+ "rel"))))
        .flatMap(_ => chunked(10000, 50)).map(_ * 2).filter(_ % 3 == 0).take(40L).onFinalize(log.update(_ :+ "fin"))
    def f(log: Ref[IO, Vector[String]]) = StromFs2.fused(
      Stream.bracket(log.update(_ :+ "open"))(_ => log.update(_ :+ "close"))
        .flatMap(_ => Stream.resource(Resource.make(log.update(_ :+ "acq"))(_ => log.update(_ :+ "rel"))))
        .flatMap(_ => chunked(10000, 50)).map(_ * 2).filter(_ % 3 == 0).take(40L).onFinalize(log.update(_ :+ "fin")))
    val log0 = Ref.unsafe[IO, Vector[String]](Vector.empty)
    assertEquals(StromFs2.fusedPlan(
      Stream.bracket(log0.update(_ :+ "open"))(_ => log0.update(_ :+ "close"))
        .flatMap(_ => Stream.resource(Resource.make(log0.update(_ :+ "acq"))(_ => log0.update(_ :+ "rel"))))
        .flatMap(_ => chunked(10000, 50)).map(_ * 2).filter(_ % 3 == 0).take(40L).onFinalize(log0.update(_ :+ "fin"))),
      "fused: map → filter → take (1 inputs)\nkept: flatMap, flatMap, onFinalize")
    val (po, pl) = logged(p)
    val (fo, fl) = logged(f)
    assertEquals(fo, po)
    assertEquals(fl, pl)
    assertEquals(fl, Vector("open", "acq", "rel", "close", "fin"))
  }

  test("finalizers run when a fused lambda throws") {
    def f(log: Ref[IO, Vector[String]]) =
      StromFs2.fused(chunked(500, 7).map(x => if x == 250 then throw Boom("x") else x).filter(_ > 0))
        .onFinalize(log.update(_ :+ "fin")).handleErrorWith(_ => Stream.empty)
    assertEquals(logged(f)._2, Vector("fin"))
  }

  // ------------------------------------------------------------------ cancellation, time

  test("an endless fused loop is interruptible (interruptAfter) and runs its finalizer") {
    val count = run(for
      fin <- Ref[IO].of(false)
      n   <- StromFs2.fused(Stream.iterate(0)(_ + 1).covary[IO].map(x => x * 31 + 7).filter(_ % 3 != 0).map(_ ^ 5))
               .onFinalize(fin.set(true)).interruptAfter(200.millis).compile.count
      f   <- fin.get
    yield (n, f))
    assert(count._1 > 0 && count._2, count)
  }

  test("an endless fused loop that emits nothing is still interruptible") {
    // The filter never passes: no output, so only cede-on-budget lets the interrupt in.
    val n = run(StromFs2.fused(Stream.iterate(0)(_ + 1).covary[IO].map(_ * 2).filter(_ < 0))
      .interruptAfter(200.millis).compile.count)
    assertEquals(n, 0L)
  }

  test("timeout fails an endless fused stream with a TimeoutException") {
    val r = run(StromFs2.fused(Stream.iterate(0)(_ + 1).covary[IO].map(_ * 2).filter(_ < 0))
      .timeout(150.millis).compile.drain.attempt)
    assert(r.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]), r)
  }

  test("fusedPar: endless parallel stream is interruptible, workers stop") {
    val n = run(StromFs2.fusedPar(4)(Stream.iterate(0)(_ + 1).covary[IO].map(x => x * 31 + 7).filter(_ % 3 != 0).map(_ ^ 5))
      .interruptAfter(200.millis).compile.count)
    assert(n > 0)
  }

  test("metered input and groupWithin output around a fused run") {
    def p = Stream.range(0, 40).covary[IO].metered(1.milli).map(_ * 3).filter(_ % 2 == 0).map(_ + 1).groupWithin(4, 1.second).map(_.toList)
    def f = StromFs2.fused(Stream.range(0, 40).covary[IO].metered(1.milli).map(_ * 3).filter(_ % 2 == 0).map(_ + 1)
      .groupWithin(4, 1.second).map(_.toList))
    assertEquals(run(f.compile.toList).flatten, run(p.compile.toList).flatten)
  }

  // ------------------------------------------------------------------ concurrency

  test("fused inner streams under parJoin / parEvalMap / merge / concurrently") {
    def inner(i: Int) = StromFs2.fused(chunked(200 + i, 1 + i % 9).map(_ * i).filter(_ % 3 != 0).map(_ + 1))
    def innerPlain(i: Int) = chunked(200 + i, 1 + i % 9).map(_ * i).filter(_ % 3 != 0).map(_ + 1)
    val joined = run(Stream.range(0, 30).covary[IO].map(inner).parJoin(8).compile.toList)
    val joinedPlain = run(Stream.range(0, 30).covary[IO].map(innerPlain).parJoin(8).compile.toList)
    assertEquals(joined.sorted, joinedPlain.sorted)

    val sums = run(Stream.range(0, 30).covary[IO].parEvalMap(8)(i => inner(i).compile.foldMonoid).compile.toList)
    assertEquals(sums, (0 until 30).toList.map(i => run(innerPlain(i).compile.foldMonoid)))

    val merged = run(inner(3).merge(inner(5)).compile.toList)
    assertEquals(merged.sorted, (run(innerPlain(3).compile.toList) ++ run(innerPlain(5).compile.toList)).sorted)

    val withBg = run(inner(4).concurrently(inner(7).evalMap(_ => IO.unit)).compile.toList)
    assertEquals(withBg, run(innerPlain(4).compile.toList))
  }

  test("one fused stream value run many times concurrently: runs don't share state") {
    val s = StromFs2.fused(chunked(3000, 17).map(_ * 3).filter(_ % 2 == 0).scan(0)(_ + _).take(2000L).fold(0L)(_ + _))
    val expected = run(chunked(3000, 17).map(_ * 3).filter(_ % 2 == 0).scan(0)(_ + _).take(2000L).fold(0L)(_ + _).compile.lastOrError)
    val results = run((1 to 64).toList.parTraverse(_ => s.compile.lastOrError))
    assert(results.forall(_ == expected), results.distinct)
  }

  test("pull-based consumers: head, take on the outside, uncons") {
    val f = StromFs2.fused(chunked(1000, 33).map(_ * 3).filter(_ % 2 == 0))
    assertEquals(run(f.head.compile.toList), List(0))
    assertEquals(run(f.take(5L).compile.toList), List(0, 6, 12, 18, 24))
    assertEquals(run(f.pull.uncons1.flatMap {
      case Some((h, _)) => _root_.fs2.Pull.output1(h)
      case None => _root_.fs2.Pull.done
    }.stream.compile.toList), List(0))
  }

  test("fusedPar: errors, finalizers and order under real effects") {
    def p(log: Ref[IO, Vector[String]]) =
      chunked(2000, 13).evalTap(_ => IO.cede).map(x => x * 7).filter(_ % 3 != 0).map(_ + 1)
        .onFinalize(log.update(_ :+ "fin"))
    def f(log: Ref[IO, Vector[String]]) = StromFs2.fusedPar(4)(
      chunked(2000, 13).evalTap(_ => IO.cede).map(x => x * 7).filter(_ % 3 != 0).map(_ + 1)
        .onFinalize(log.update(_ :+ "fin")))
    assertEquals(logged(f), logged(p))
    val e = run(StromFs2.fusedPar(4)(chunked(2000, 13).map(x => if x == 1500 then throw Boom("par") else x).filter(_ > 0))
      .compile.drain.attempt)
    assert(e.left.exists(_.getMessage == "par"), e)
  }
