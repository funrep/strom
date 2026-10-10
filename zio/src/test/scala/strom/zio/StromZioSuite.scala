package strom.zio

import scala.util.Random
import _root_.zio.{Chunk, Runtime, Unsafe, ZIO}
import _root_.zio.stream.ZStream
import strom.{Pipelines, Reference, Strom}

class StromZioSuite extends munit.FunSuite:
  def unsafeRun[A](z: ZIO[Any, Throwable, A]): A = Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z).getOrThrow())

  def chunked(xs: Array[Int], r: Random): ZStream[Any, Nothing, Int] =
    ZStream.fromChunks(xs.grouped(1 + r.nextInt(9)).map(Chunk.fromArray).toSeq*)

  test("pipeline agrees with the reference") {
    val r = Random(5)
    val f = Strom.segment(Pipelines.threeLevels)
    for _ <- 0 until 100 do
      val xs = Array.fill(r.nextInt(40))(r.nextInt(30))
      val got = unsafeRun(chunked(xs, r).via(StromZio.pipeline[Int, Int](f, 1 + r.nextInt(5), 1 + r.nextInt(30))).runCollect)
      assertEquals(got.toList, Reference.toList(Pipelines.threeLevels(Strom.fromSeq(xs.toSeq))))
  }

  test("fold agrees with the reference") {
    val r = Random(6)
    val f = Strom.foldSegment(Pipelines.longSum)
    for _ <- 0 until 100 do
      val xs = Array.fill(r.nextInt(40))(r.nextInt(30) - 10)
      assertEquals(unsafeRun(StromZio.runFold[Any, Nothing, Int, Long](f, 1 + r.nextInt(9))(chunked(xs, r))),
        Reference.run(Pipelines.longSum(Strom.fromSeq(xs.toSeq))))
  }
