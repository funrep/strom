package strom.zio

import scala.util.Random
import _root_.zio.{Chunk, Runtime, Unsafe, ZIO}
import _root_.zio.stream.ZStream

class ZioFusedSuite extends munit.FunSuite:
  def unsafeRun[A](z: ZIO[Any, Throwable, A]): A = Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z).getOrThrow())
  def chunked(xs: Array[Int], r: Random): ZStream[Any, Nothing, Int] =
    ZStream.fromChunks(xs.grouped(1 + r.nextInt(9)).map(Chunk.fromArray).toSeq*)

  def same[A](name: String)(plain: ZStream[Any, Nothing, Int] => ZStream[Any, Throwable, A],
                            fusedS: ZStream[Any, Nothing, Int] => ZStream[Any, Throwable, A]): Unit =
    test(name) {
      val r = Random(name.hashCode)
      for _ <- 0 until 100 do
        val xs = Array.fill(r.nextInt(80))(r.nextInt(40) - 5)
        val seed = r.nextLong()
        assertEquals(unsafeRun(fusedS(chunked(xs, Random(seed))).runCollect).toList,
          unsafeRun(plain(chunked(xs, Random(seed))).runCollect).toList)
    }

  same("map/filter/take")(
    s => s.map(_ * 3).filter(_ % 2 == 0).take(20),
    s => StromZio.fused(s.map(_ * 3).filter(_ % 2 == 0).take(20)))

  same("effects stay between fused runs")(
    s => s.map(_ + 1).filter(_ > 0).mapZIO(x => ZIO.attempt(x * 2)).map(_ - 1).filter(_ % 3 != 0).rechunk(3).map(_ * 5),
    s => StromZio.fused(s.map(_ + 1).filter(_ > 0).mapZIO(x => ZIO.attempt(x * 2)).map(_ - 1).filter(_ % 3 != 0).rechunk(3).map(_ * 5)))

  same("flatMap onto ranges")(
    s => s.flatMap(x => ZStream.range(0, x % 5).map(_ * 2).filter(_ != 4)).map(_ + 1),
    s => StromZio.fused(s.flatMap(x => ZStream.range(0, x % 5).map(_ * 2).filter(_ != 4)).map(_ + 1)))

  same("collect / zipWithIndex / scan / drop / takeWhile / dropWhile")(
    s => s.collect { case x if x % 2 == 0 => x * 3 }.zipWithIndex.map((x, i) => x + i.toInt).dropWhile(_ < 3)
      .scan(0L)(_ + _).drop(2).takeWhile(_ < 900L),
    s => StromZio.fused(s.collect { case x if x % 2 == 0 => x * 3 }.zipWithIndex.map((x, i) => x + i.toInt).dropWhile(_ < 3)
      .scan(0L)(_ + _).drop(2).takeWhile(_ < 900L)))

  same("mapAccum")(
    s => s.mapAccum(0)((acc, x) => (acc + x, acc * 2)).filter(_ % 3 != 0),
    s => StromZio.fused(s.mapAccum(0)((acc, x) => (acc + x, acc * 2)).filter(_ % 3 != 0)))

  same("zipWith another stream and range sources")(
    s => s.map(_ * 2).zipWith(ZStream.range(0, 1000).filter(_ % 3 == 0))(_ - _).take(40),
    s => StromZio.fused(s.map(_ * 2).zipWith(ZStream.range(0, 1000).filter(_ % 3 == 0))(_ - _).take(40)))

  test("fusion actually happens") {
    def s: ZStream[Any, Nothing, Int] = ZStream.empty
    assertEquals(StromZio.fusedPlan(s.map(_ + 1).filter(_ > 0).mapZIO(x => ZIO.succeed(x)).map(_ * 2).take(9)),
      "fused: map → filter (1 inputs)\nfused: map → take (1 inputs)\nkept: mapZIO")
    assertEquals(StromZio.fusedPlan(s.map(_ + 1).zipWith(ZStream.range(0, 9).map(_ * 2))(_ + _)),
      "fused: map → zipWith (1 inputs)")
  }

  same("flatMap onto succeed / fromIterable / fromChunk")(
    s => s.flatMap(x => ZStream.succeed(x * 2)).flatMap(x => ZStream.fromIterable(List(x, x + 1).filter(_ % 3 != 0)))
      .flatMap(x => ZStream.fromChunk(Chunk(x, -x))).map(_ + 1),
    s => StromZio.fused(s.flatMap(x => ZStream.succeed(x * 2)).flatMap(x => ZStream.fromIterable(List(x, x + 1).filter(_ % 3 != 0)))
      .flatMap(x => ZStream.fromChunk(Chunk(x, -x))).map(_ + 1)))

