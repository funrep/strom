package strom.zio

import scala.util.Random
import _root_.zio.{Chunk, Runtime, Unsafe, ZIO}
import _root_.zio.stream.ZStream

/** `StromZio.fusedPar` must produce exactly what the plain (sequential, unfused) stream does. */
class ZioParallelSuite extends munit.FunSuite:
  def unsafeRun[A](z: ZIO[Any, Throwable, A]): A = Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z).getOrThrow())
  def chunked(xs: Array[Int], r: Random): ZStream[Any, Nothing, Int] =
    ZStream.fromChunks(xs.grouped(1 + r.nextInt(30)).map(Chunk.fromArray).toSeq*)

  def same[A](name: String)(plain: ZStream[Any, Nothing, Int] => ZStream[Any, Nothing, A],
      par: (ZStream[Any, Nothing, Int], Int) => ZStream[Any, Nothing, A]): Unit =
    test(name) {
      val r = Random(name.hashCode)
      for _ <- 0 until 30 do
        val xs = Array.fill(r.nextInt(2000))(r.nextInt(50) - 5)
        val (seed, n) = (r.nextLong(), 1 + r.nextInt(6))
        assertEquals(unsafeRun(par(chunked(xs, Random(seed)), n).runCollect).toList,
          unsafeRun(plain(chunked(xs, Random(seed))).runCollect).toList)
    }

  same("stateless: replicated")(
    s => s.map(_ * 3).filter(_ % 2 == 0).flatMap(x => ZStream.range(0, x % 6)),
    (s, n) => StromZio.fusedPar(n)(s.map(_ * 3).filter(_ % 2 == 0).flatMap(x => ZStream.range(0, x % 6))))

  same("cut at take")(
    s => s.flatMap(x => ZStream.range(0, x % 5)).filter(_ != 2).take(300).map(_ + 1),
    (s, n) => StromZio.fusedPar(n)(s.flatMap(x => ZStream.range(0, x % 5)).filter(_ != 2).take(300).map(_ + 1)))

  same("multi-cut")(
    s => s.flatMap(x => ZStream.range(0, x % 4)).take(700).map(_ * 3).filter(_ % 2 == 0).flatMap(x => ZStream.range(0, x % 3)).map(_ + 1),
    (s, n) => StromZio.fusedPar(n)(
      s.flatMap(x => ZStream.range(0, x % 4)).take(700).map(_ * 3).filter(_ % 2 == 0).flatMap(x => ZStream.range(0, x % 3)).map(_ + 1)))

  same("effects between parallel runs")(
    s => s.map(_ + 1).filter(_ > 0).mapZIO(x => ZIO.succeed(x * 2)).map(_ - 1).filter(_ % 3 != 0).scan(0)(_ + _),
    (s, n) => StromZio.fusedPar(n)(s.map(_ + 1).filter(_ > 0).mapZIO(x => ZIO.succeed(x * 2)).map(_ - 1).filter(_ % 3 != 0).scan(0)(_ + _)))

  val src: ZStream[Any, Nothing, Int] = ZStream.empty

  same("zip with an effectful stream: runs around it in parallel")(
    s => s.map(_ * 3).filter(_ % 2 == 0).zipWith(ZStream.range(0, 5000).mapZIO(ZIO.succeed(_)).map(_ + 1))(_ - _).map(_ * 7).filter(_ > 3),
    (s, n) => StromZio.fusedPar(n)(
      s.map(_ * 3).filter(_ % 2 == 0).zipWith(ZStream.range(0, 5000).mapZIO(ZIO.succeed(_)).map(_ + 1))(_ - _).map(_ * 7).filter(_ > 3)))

  test("plans") {
    assertEquals(StromZio.fusedParPlan(src.map(_ * 2).filter(_ > 0).take(5).map(_ + 1)),
      "fused: map → filter → take → map (1 inputs), parallel: replicated[map → filter] | single[take → map]")
  }

  test("a captured var is a compile error") {
    val errs = compileErrors("{ var k = 0; StromZio.fusedPar(2)(src.map(x => { k += 1; x + k })) }")
    assert(errs.contains("data race"), errs)
  }
