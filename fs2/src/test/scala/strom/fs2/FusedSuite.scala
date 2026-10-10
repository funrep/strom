package strom.fs2

import scala.util.Random
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import _root_.fs2.{Chunk, Pure, Stream}

/** `StromFs2.fused` rewrites plain fs2 code; results must equal the unfused expression. */
class FusedSuite extends munit.FunSuite:
  def chunked(xs: Array[Int], r: Random): Stream[IO, Int] =
    Stream.emits(xs.grouped(1 + r.nextInt(9)).map(c => if r.nextBoolean() then Chunk.array(c) else Chunk.from(c.toVector)).toSeq)
      .flatMap(Stream.chunk).covary[IO]

  def same[A](name: String)(plain: Stream[IO, Int] => Stream[IO, A], fusedS: Stream[IO, Int] => Stream[IO, A]): Unit =
    test(name) {
      val r = Random(name.hashCode)
      for _ <- 0 until 150 do
        val xs = Array.fill(r.nextInt(80))(r.nextInt(40) - 5)
        val seed = r.nextLong()
        assertEquals(fusedS(chunked(xs, Random(seed))).compile.toList.unsafeRunSync(),
          plain(chunked(xs, Random(seed))).compile.toList.unsafeRunSync())
    }

  same("map/filter/take")(
    s => s.map(_ * 3).filter(_ % 2 == 0).take(20L),
    s => StromFs2.fused(s.map(_ * 3).filter(_ % 2 == 0).take(20L)))

  same("effects stay between fused runs")(
    s => s.map(_ + 1).filter(_ > 0).evalMap(x => IO(x * 2)).map(_ - 1).filter(_ % 3 != 0).chunkLimit(3).unchunks.map(_ * 5).take(30L),
    s => StromFs2.fused(s.map(_ + 1).filter(_ > 0).evalMap(x => IO(x * 2)).map(_ - 1).filter(_ % 3 != 0).chunkLimit(3).unchunks.map(_ * 5).take(30L)))

  same("flatMap onto ranges")(
    s => s.flatMap(x => Stream.range(0, x % 5).map(_ * 2).filter(_ != 4)).map(_ + 1),
    s => StromFs2.fused(s.flatMap(x => Stream.range(0, x % 5).map(_ * 2).filter(_ != 4)).map(_ + 1)))

  same("zipWith another (fused) stream")(
    s => s.map(_ * 2).zipWith(Stream.range(0, 1000).covary[IO].filter(_ % 3 == 0).map(_ + 1))(_ - _).take(40L),
    s => StromFs2.fused(s.map(_ * 2).zipWith(Stream.range(0, 1000).covary[IO].filter(_ % 3 == 0).map(_ + 1))(_ - _).take(40L)))

  same("fold at the end")(
    s => s.map(_.toLong).filter(_ > 3).fold(0L)(_ * 7 + _),
    s => StromFs2.fused(s.map(_.toLong).filter(_ > 3).fold(0L)(_ * 7 + _)))

  same("unfusable operators only")(
    s => s.chunkLimit(2).unchunks.evalMap(x => IO(x)),
    s => StromFs2.fused(s.chunkLimit(2).unchunks.evalMap(x => IO(x))))

  same("collect / mapFilter (cats) / zipWithIndex")(
    s => s.collect { case x if x % 2 == 0 => x * 3 }.mapFilter(x => Option.when(x > 4)(x - 1)).zipWithIndex.map((x, i) => x + i.toInt),
    s => StromFs2.fused(s.collect { case x if x % 2 == 0 => x * 3 }.mapFilter(x => Option.when(x > 4)(x - 1)).zipWithIndex.map((x, i) => x + i.toInt)))

  same("zip / zipWithIndex with untupled lambdas")(
    s => s.zip(Stream.range(0, 500).covary[IO]).map((x, y) => x * y).zipWithIndex.collect { case (v, i) if i % 2 == 0 => v },
    s => StromFs2.fused(s.zip(Stream.range(0, 500).covary[IO]).map((x, y) => x * y).zipWithIndex.collect { case (v, i) if i % 2 == 0 => v }))

  same("scan / drop / takeWhile / dropWhile")(
    s => s.dropWhile(_ < 3).scan(0L)(_ + _).drop(2L).takeWhile(_ < 900L).map(_ * 2),
    s => StromFs2.fused(s.dropWhile(_ < 3).scan(0L)(_ + _).drop(2L).takeWhile(_ < 900L).map(_ * 2)))

  same("takeWhile with takeFailure = true is kept")(
    s => s.map(_ + 1).takeWhile(_ < 30, true).map(_ * 2),
    s => StromFs2.fused(s.map(_ + 1).takeWhile(_ < 30, true).map(_ * 2)))

  test("pure streams") {
    val plain = Stream.range(0, 1000).map(_ * 3).filter(_ % 7 != 0).take(500L).toList
    val fusedL = StromFs2.fused(Stream.range(0, 1000).map(_ * 3).filter(_ % 7 != 0).take(500L)).toList
    assertEquals(fusedL, plain)
  }

  test("fusion actually happens") {
    def s: Stream[IO, Int] = Stream.empty
    val plan = StromFs2.fusedPlan(s.map(_ + 1).filter(_ > 0).evalMap(x => IO(x)).map(_ * 2).filter(_ < 9).fold(0)(_ + _))
    assertEquals(plan, "fused: map → filter (1 inputs)\nfused: map → filter → fold (1 inputs)\nkept: evalMap")
    assertEquals(StromFs2.fusedPlan(Stream.range(0, 9).map(_ + 1).take(3L)), "fused: map → take (0 inputs), never cedes")
    assertEquals(StromFs2.fusedPlan(s.collect { case x if x > 1 => x }.mapFilter(Option(_)).zipWithIndex.scan(0)((a, t) => a + t._1)),
      "fused: collect → mapFilter → zipWithIndex → scan (1 inputs)")
    // takeWhile's default argument makes the compiler lift the call into synthetic vals.
    assertEquals(StromFs2.fusedPlan(s.map(_ + 1).takeWhile(_ < 30).map(_ * 2)), "fused: map → takeWhile → map (1 inputs)")
    // A lone map is not worth a segment boundary.
    assertEquals(StromFs2.fusedPlan(s.map(_ + 1).evalMap(x => IO(x))), "kept: evalMap")
  }
