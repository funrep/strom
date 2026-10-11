package strom.fs2

import scala.util.Random
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import _root_.fs2.{Chunk, Stream}

/** `StromFs2.fusedPar` must produce exactly what the plain (sequential, unfused) stream does. */
class Fs2ParallelSuite extends munit.FunSuite:
  def chunked(xs: Array[Int], r: Random): Stream[IO, Int] =
    Stream.emits(xs.grouped(1 + r.nextInt(30)).map(c => if r.nextBoolean() then Chunk.array(c) else Chunk.from(c.toVector)).toSeq)
      .flatMap(Stream.chunk).covary[IO]

  def same[A](name: String)(plain: Stream[IO, Int] => Stream[IO, A], par: (Stream[IO, Int], Int) => Stream[IO, A]): Unit =
    test(name) {
      val r = Random(name.hashCode)
      for _ <- 0 until 40 do
        val xs = Array.fill(r.nextInt(2000))(r.nextInt(50) - 5)
        val (seed, n) = (r.nextLong(), 1 + r.nextInt(6))
        assertEquals(par(chunked(xs, Random(seed)), n).compile.toList.unsafeRunSync(),
          plain(chunked(xs, Random(seed))).compile.toList.unsafeRunSync())
    }

  same("stateless: replicated")(
    s => s.map(_ * 3).filter(_ % 2 == 0).flatMap(x => Stream.range(0, x % 6)),
    (s, n) => StromFs2.fusedPar(n)(s.map(_ * 3).filter(_ % 2 == 0).flatMap(x => Stream.range(0, x % 6))))

  same("cut at take")(
    s => s.flatMap(x => Stream.range(0, x % 5)).filter(_ != 2).take(300L).map(_ + 1),
    (s, n) => StromFs2.fusedPar(n)(s.flatMap(x => Stream.range(0, x % 5)).filter(_ != 2).take(300L).map(_ + 1)))

  same("associative fold: partial folds")(
    s => s.map(x => x * x).filter(_ % 3 != 0).fold(0)(_ + _),
    (s, n) => StromFs2.fusedPar(n)(s.map(x => x * x).filter(_ % 3 != 0).fold(0)(_ + _)))

  same("max fold")(
    s => s.map(_ * 7 % 13).fold(Int.MinValue)(math.max),
    (s, n) => StromFs2.fusedPar(n)(s.map(_ * 7 % 13).fold(Int.MinValue)(math.max)))

  same("non-associative fold: single")(
    s => s.map(_ * 2).fold(7)((acc, x) => acc * 31 + x),
    (s, n) => StromFs2.fusedPar(n)(s.map(_ * 2).fold(7)((acc, x) => acc * 31 + x)))

  same("multi-cut")(
    s => s.flatMap(x => Stream.range(0, x % 4)).take(700L).map(_ * 3).filter(_ % 2 == 0).flatMap(x => Stream.range(0, x % 3)).map(_ + 1),
    (s, n) => StromFs2.fusedPar(n)(
      s.flatMap(x => Stream.range(0, x % 4)).take(700L).map(_ * 3).filter(_ % 2 == 0).flatMap(x => Stream.range(0, x % 3)).map(_ + 1)))

  same("multi-cut ending in a partial fold")(
    s => s.map(_ + 1).take(900L).flatMap(x => Stream.range(0, x % 5)).map(_ * 2).fold(0L)((a, b) => a + b),
    (s, n) => StromFs2.fusedPar(n)(s.map(_ + 1).take(900L).flatMap(x => Stream.range(0, x % 5)).map(_ * 2).fold(0L)((a, b) => a + b)))

  same("effects between parallel runs")(
    s => s.map(_ + 1).filter(_ > 0).evalMap(x => IO(x * 2)).map(_ - 1).filter(_ % 3 != 0).scan(0)(_ + _),
    (s, n) => StromFs2.fusedPar(n)(s.map(_ + 1).filter(_ > 0).evalMap(x => IO(x * 2)).map(_ - 1).filter(_ % 3 != 0).scan(0)(_ + _)))

  test("empty input") {
    assertEquals(StromFs2.fusedPar(3)(Stream.empty.covaryAll[IO, Int].map(_ * 2).fold(0)(_ + _)).compile.toList.unsafeRunSync(), List(0))
  }

  val src: Stream[IO, Int] = Stream.empty

  test("plans") {
    assertEquals(StromFs2.fusedParPlan(src.map(_ * 2).filter(_ > 0).take(5L).map(_ + 1).fold(0)(_ + _)),
      "fused: map → filter → take → map → fold (1 inputs), parallel: replicated[map → filter] | single[take → map → fold]")
    assertEquals(StromFs2.fusedParPlan(src.map(_ * 2).fold(0)(_ + _)),
      "fused: map → fold (1 inputs), parallel: partialFold[map → fold]")
    assertEquals(StromFs2.fusedParPlan(src.take(5L).scan(0)(_ + _)),
      "fused: take → scan (1 inputs)")
  }

  test("a captured var is a compile error") {
    val errs = compileErrors("{ var k = 0; StromFs2.fusedPar(2)(src.map(x => { k += 1; x + k })) }")
    assert(errs.contains("data race"), errs)
    assertEquals(compileErrors("{ var k = 0; StromFs2.fused(src.map(x => { k += 1; x + k })) }"), "")
    assertEquals(compileErrors("StromFs2.fusedPar(2)(src.map(x => { var k = x; k += 1; k }))"), "")
  }

  test("a failure propagates") {
    val s = StromFs2.fusedPar(3)(Stream.range(0, 5000).covary[IO].map(x => 100 / (x - 4000)))
    intercept[ArithmeticException](s.compile.drain.unsafeRunSync())
  }
