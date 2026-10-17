package strom.effect

import zio.{Exit, Ref, Runtime, Unsafe, ZIO, durationInt}
import scala.collection.mutable.ListBuffer

/** Differential tests for ZIO: each program runs plain and fused; the Exit (success, typed failure
 *  or defect) and the effect trace must be identical. */
class ZioFusedSuite extends munit.FunSuite:
  final class Boom(msg: String) extends RuntimeException(msg)

  def unsafeRun[A](z: ZIO[Any, Nothing, A]): A =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z).getOrThrowFiberFailure())

  def outcome[E, A](z: ZIO[Any, E, A]): String = unsafeRun(z.exit) match
    case Exit.Success(a) => s"ok $a"
    case Exit.Failure(c) =>
      c.failureOption.map(e => s"fail $e")
        .orElse(c.dieOption.map(t => s"die ${t.getClass.getSimpleName} ${t.getMessage}"))
        .getOrElse(s"other $c")

  inline def same[E, A](inline prog: ListBuffer[String] => ZIO[Any, E, A])(using munit.Location): String =
    val pl = ListBuffer.empty[String]
    val fl = ListBuffer.empty[String]
    val p = outcome(prog(pl))
    val f = outcome(StromEffect.fused(prog(fl)))
    assertEquals(f, p)
    assertEquals(fl.toList, pl.toList)
    p

  def opaque(log: ListBuffer[String], s: String): ZIO[Any, Nothing, Unit] = ZIO.succeed(log += s).unit
  def lookup(i: Int): ZIO[Any, Nothing, Int] = ZIO.succeed(i * 10)

  test("straight-line for-comprehension") {
    assertEquals(same(log =>
      for
        a <- ZIO.succeed(3)
        b <- ZIO.succeed(log += s"b $a").as(a * 2)
        _ <- ZIO.unit
        c <- ZIO.succeed { log += "c"; b + 1 }.map(_ * 3)
        e <- if c > 10 then ZIO.succeed(log += "big").as(c) else ZIO.succeed(-c)
      yield e + b), "ok 27")
  }

  test("opaque calls in between, zip operators") {
    same(log =>
      for
        a <- ZIO.succeed(1)
        _ <- opaque(log, s"one $a")
        b <- lookup(a) <* ZIO.succeed(log += "after lookup")
        c <- ZIO.succeed(log += "x") *> lookup(b).zipRight(ZIO.succeed(b + 1))
        _ <- opaque(log, "two").unit
      yield (a, b, c))
  }

  test("typed failures: ZIO.fail in sync code, caught by catchAll and either") {
    assertEquals(same(log => ZIO.succeed(log += "a") *> ZIO.fail("bad") *> ZIO.succeed(log += "never").as(1)), "fail bad")
    assertEquals(same(log => (ZIO.succeed(log += "a") *> ZIO.fail("x")).catchAll(e => ZIO.succeed(log += e).as(-1))), "ok -1")
    assertEquals(same(log => (lookup(2) *> ZIO.fail("y")).catchAll(e => opaque(log, e).as(-2))), "ok -2")
    assertEquals(same(log => (ZIO.succeed(log += "b") *> ZIO.fail("z")).either), "ok Left(z)")
    assertEquals(same(log => lookup(3).flatMap(_ => ZIO.fail("w")).either.map(_.isLeft)), "ok true")
    // A failure after the handled region is not caught by it.
    assertEquals(same(log => (ZIO.succeed(1): ZIO[Any, String, Int]).catchAll(_ => ZIO.succeed(2)).flatMap(_ => ZIO.fail("later"))), "fail later")
  }

  test("defects: a throw is a defect, not caught by catchAll; attempt makes it a failure") {
    assertEquals(same(log => (ZIO.succeed[Int](throw Boom("d")): ZIO[Any, String, Int]).catchAll(_ => ZIO.succeed(0))), "die Boom d")
    assertEquals(same(log => (ZIO.succeed(1).map(x => if x == 1 then throw Boom("map") else x): ZIO[Any, String, Int]).catchAll(_ => ZIO.succeed(0))),
      "die Boom map")
    assertEquals(same(log => ZIO.attempt[Int](throw Boom("a")).catchAll(e => ZIO.succeed(log += e.getMessage).as(5))), "ok 5")
    assertEquals(same(log => ZIO.attempt[Int](throw Boom("b"))), "fail strom.effect.ZioFusedSuite$Boom: b")
    assertEquals(same(log => ZIO.succeed(log += "x") *> ZIO.die(Boom("died"))), "die Boom died")
  }

  test("sync loop: one million iterations, stack safe") {
    assertEquals(same(_ =>
      def loop(i: Int, acc: Long): ZIO[Any, Nothing, Long] =
        if i >= 1000000 then ZIO.succeed(acc)
        else ZIO.succeed(i * 3L).flatMap(x => loop(i + 1, acc + x))
      loop(0, 0L)), s"ok ${3L * 999999L * 1000000L / 2}")
  }

  test("loop with an opaque Ref; a typed failure from a loop that suspends") {
    same(log =>
      Ref.make(0).flatMap { ref =>
        def loop(i: Int): ZIO[Any, String, Int] =
          if i == 5000 then ref.get
          else
            for
              x <- ZIO.succeed(i % 7)
              _ <- ref.update(_ + x)
              _ <- if i % 1000 == 0 then ZIO.succeed(log += s"at $i") else ZIO.unit
              _ <- if i == 4321 then ZIO.fail(s"stop $i") else ZIO.unit
              r <- loop(i + 1)
            yield r
        loop(0)
      })
  }

  test("catchAll inside a loop; match with binds across an opaque call") {
    same(log =>
      def loop(i: Int, acc: Int): ZIO[Any, Nothing, Int] =
        if i == 2000 then ZIO.succeed(acc)
        else
          (if i % 500 == 499 then ZIO.fail(s"at $i") else ZIO.succeed(i))
            .catchAll(e => ZIO.succeed(log += e).as(0))
            .map(x => (x, x * 2))
            .flatMap { case (a, b) => lookup(a).flatMap(y => loop(i + 1, acc + y + b)) }
      loop(0, 0))
  }

  test("a local def used as a value stays lazy") {
    same(log =>
      def loop(i: Int): ZIO[Any, Nothing, Int] =
        if i == 3 then ZIO.succeed(i) else ZIO.succeed(log += s"step $i").flatMap(_ => loop(i + 1))
      val x = loop(0)
      ZIO.succeed(log += "start") *> x *> x)
  }

  test("endless compiled loops yield: race and timeout end them") {
    def spin(c: java.util.concurrent.atomic.AtomicLong): ZIO[Any, Nothing, Unit] = StromEffect.fused {
      def loop(i: Long): ZIO[Any, Nothing, Unit] = ZIO.succeed(c.incrementAndGet()).flatMap(_ => loop(i + 1))
      loop(0)
    }
    val (a, b) = (java.util.concurrent.atomic.AtomicLong(), java.util.concurrent.atomic.AtomicLong())
    val r = unsafeRun(spin(a).race(spin(b)).timeout(300.millis))
    assertEquals(r, None)
    assert(a.get > 0 && b.get > 0, (a.get, b.get))
  }

  test("a synchronous part with a handler that is not: recover by looping, fail in the handler, suspend") {
    assertEquals(same(log =>
      def loop(i: Int): ZIO[Any, Nothing, Int] =
        if i < 200000 then (ZIO.fail("x").map((_: Int) + 1): ZIO[Any, String, Int]).catchAll(_ => loop(i + 1))
        else ZIO.succeed(i)
      loop(0)), "ok 200000")
    same(log =>
      def loop(i: Int, acc: Int): ZIO[Any, Nothing, Int] =
        if i == 3000 then ZIO.succeed(acc)
        else (if i % 7 == 0 then ZIO.fail(s"b$i") else ZIO.succeed(i))
          .catchAll(e => opaque(log, e).as(-1))
          .flatMap(x => loop(i + 1, acc + x))
      loop(0, 0))
    assertEquals(same(log =>
      (ZIO.succeed(log += "a") *> ZIO.fail("first"))
        .catchAll(e => opaque(log, e) *> ZIO.fail("second"))
        .catchAll(e => ZIO.succeed(log += s"outer $e").as(7))), "ok 7")
    // A defect is not caught by catchAll, also when the handler is not synchronous.
    assertEquals(same(log => (ZIO.succeed[Int](throw Boom("d")): ZIO[Any, String, Int]).catchAll(_ => lookup(1))), "die Boom d")
  }
