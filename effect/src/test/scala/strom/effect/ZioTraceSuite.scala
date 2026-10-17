package strom.effect

import zio.{Cause, Runtime, Trace, Unsafe, ZIO}

/** The calls the fused code makes carry the `Trace` of the user's calls they stand for, so a
 *  failure's stack trace names the user's lines. */
class ZioTraceSuite extends munit.FunSuite:
  def lookup(i: Int): ZIO[Any, Nothing, Int] = ZIO.succeed(i * 10)
  def failing(i: Int): ZIO[Any, String, Int] = ZIO.fail(s"bad $i")

  /** The lines of this file in the failure's stack trace. */
  def lines[A](z: ZIO[Any, String, A]): List[Int] =
    val cause: Cause[String] = Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z.sandbox.flip).getOrThrowFiberFailure())
    cause.traces.flatMap(_.stackTrace.toList).collect { case Trace(_, file, line) if file == "ZioTraceSuite.scala" => line }.distinct.sorted

  inline def both[A](inline z: ZIO[Any, String, A]): (List[Int], List[Int]) = (lines(z), lines(StromEffect.fused(z)))

  test("flatMap continuations pending at a failure have the user's lines") {
    val (plain, fused) = both(
      for
        a <- lookup(1)
        x <- failing(a)
        y <- lookup(x)
        z <- lookup(y)
      yield z + 1)
    assert(plain.size >= 2, plain)
    assertEquals(fused, plain)
  }

  test("map, zipRight and catchAll") {
    val (plain, fused) = both(
      lookup(1)
        .flatMap(a => (lookup(a) *> failing(a)).map(_ + 1))
        .catchAll(e => failing(e.length))
        .flatMap(v => lookup(v)))
    assert(plain.nonEmpty, plain)
    assertEquals(fused, plain)
  }

  test("inside a loop") {
    val (plain, fused) = both {
      def loop(i: Int): ZIO[Any, String, Int] =
        if i == 3 then failing(i).flatMap(v => lookup(v))
        else lookup(i).flatMap(_ => loop(i + 1))
      loop(0).map(_ + 1)
    }
    assert(plain.nonEmpty, plain)
    assertEquals(fused, plain)
  }
