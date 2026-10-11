package strom.fs2

import scala.util.Random
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import _root_.fs2.{Chunk, Stream}
import strom.{Pipelines, Reference, Strom}

class StromFs2Suite extends munit.FunSuite:
  /** A stream of `xs` cut into random chunks (array-backed, or not, at random). */
  def chunked(xs: Array[Int], r: Random): Stream[IO, Int] =
    var s: Stream[IO, Int] = Stream.empty
    var i = 0
    while i < xs.length do
      val n = math.min(xs.length - i, 1 + r.nextInt(9))
      val c = if r.nextBoolean() then Chunk.array(xs.slice(i, i + n)) else Chunk.from(xs.slice(i, i + n).toVector)
      s = s ++ Stream.chunk(c)
      i += n
    s

  test("pipe2 (emit) agrees with the reference") {
    val f = Strom.segment(Pipelines.representativeEmit(40))
    val r = Random(1)
    for _ <- 0 until 100 do
      val a = Array.fill(r.nextInt(30))(r.nextInt(8)); val b = Array.fill(r.nextInt(60))(r.nextInt(9))
      val got = StromFs2.pipe2[IO, Int, Int, Int](f, maxOut = 1 + r.nextInt(6), budget = 1 + r.nextInt(20))(
        chunked(a, r), chunked(b, r)).compile.toList.unsafeRunSync()
      assertEquals(got, Reference.toList(Pipelines.representativeEmit(40)(Strom.fromSeq(a.toSeq), Strom.fromSeq(b.toSeq))))
  }

  test("fold2 agrees with the reference") {
    val f = Strom.segment(Pipelines.representative(25))
    val r = Random(2)
    for _ <- 0 until 100 do
      val a = Array.fill(r.nextInt(30))(r.nextInt(8)); val b = Array.fill(r.nextInt(60))(r.nextInt(9))
      val got = StromFs2.fold2[IO, Int, Int, Int](f, budget = 1 + r.nextInt(20))(chunked(a, r), chunked(b, r))
        .compile.toList.unsafeRunSync()
      assertEquals(got, List(Reference.run(Pipelines.representative(25)(Strom.fromSeq(a.toSeq), Strom.fromSeq(b.toSeq)))))
  }

  test("pipe with take stops pulling upstream early") {
    var pulled = 0
    val src = Stream.range(0, 1_000_000).covary[IO].chunkLimit(10).unchunks.evalTap(_ => IO { pulled += 1 })
    val got = src.through(StromFs2.pipe[IO, Int, Int](Strom.segment((a: Strom[Int]) => a.map(_ * 2).take(5)))).compile.toList.unsafeRunSync()
    assertEquals(got, List(0, 2, 4, 6, 8))
    assert(pulled < 100, s"pulled $pulled")
  }

  test("a stream can be run twice (fresh segment per run)") {
    val s = Stream.emits(1 to 10).covary[IO].through(StromFs2.pipe[IO, Int, Int](Strom.segment((a: Strom[Int]) => a.filter(_ % 2 == 0))))
    assertEquals(s.compile.toList.unsafeRunSync(), s.compile.toList.unsafeRunSync())
  }
