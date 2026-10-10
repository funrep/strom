package strom.zio

import java.util.concurrent.Executors
import scala.util.Random
import _root_.zio.{Chunk, Executor, Runtime, Unsafe, ZIO}
import _root_.zio.stream.ZStream
import strom.{Reference, Strom}
import strom.runtime.{KeyPartitioner, ParallelPlan}

class ZioParallelSuite extends munit.FunSuite:
  val pool = Executors.newFixedThreadPool(3)
  val compute = Executor.fromJavaExecutor(pool)
  override def afterAll(): Unit = pool.shutdown()

  def unsafeRun[A](z: ZIO[Any, Throwable, A]): A = Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z).getOrThrow())
  def chunked(xs: Array[Int], r: Random): ZStream[Any, Nothing, Int] =
    ZStream.fromChunks(xs.grouped(1 + r.nextInt(30)).map(Chunk.fromArray).toSeq*)

  def run[B](plan: ParallelPlan[B], xs: Array[Int], r: Random, n: Int): List[B] =
    val p = StromZio.parallel[Any, Nothing, Int, B](plan, n, if r.nextBoolean() then Some(compute) else None,
      maxOut = 1 + r.nextInt(64), budget = 1 + r.nextInt(500))
    unsafeRun(p(chunked(xs, r)).runCollect).toList

  inline def stateless = (a: Strom[Int]) => a.map(_ * 3).filter(_ % 2 == 0).flatMap(x => Strom.range(0, x % 6))
  inline def cut = (a: Strom[Int]) => a.flatMap(x => Strom.range(0, x % 5)).filter(_ != 2).take(300).map(_ + 1)
  inline def partial = (a: Strom[Int]) => a.map(x => x * x).filter(_ % 3 != 0).sum
  inline def nonAssoc = (a: Strom[Int]) => a.map(_ * 2).fold(7)((s, x) => s * 31 + x)
  inline def multi = (a: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x % 4)).take(700).map(_ * 3).filter(_ % 2 == 0).flatMap(x => Strom.range(0, x % 3)).map(_ + 1)
  inline def windowed = (a: Strom[Int]) => a.windowFold(2)(0)(_ * 10 + _)

  def input(r: Random) = Array.fill(r.nextInt(2000))(r.nextInt(50) - 5)

  test("replicated, cut, partial fold, single fold, multi-cut") {
    val (p1, p2, p3, p4, p5) =
      (Strom.parallel(stateless), Strom.parallel(cut), Strom.parallelFold(partial), Strom.parallelFold(nonAssoc), Strom.parallel(multi))
    val r = Random(1)
    for _ <- 0 until 30 do
      val xs = input(r); val n = 1 + r.nextInt(6); val s = Strom.fromSeq(xs.toSeq)
      assertEquals(run(p1, xs, r, n), Reference.toList(stateless(s)))
      assertEquals(run(p2, xs, r, n), Reference.toList(cut(s)))
      assertEquals(run(p3, xs, r, n), List(Reference.run(partial(s))))
      assertEquals(run(p4, xs, r, n), List(Reference.run(nonAssoc(s))))
      assertEquals(run(p5, xs, r, n), Reference.toList(multi(s)))
  }

  test("keyed") {
    val plan = Strom.parallelKeyed((x: Int) => x % 11)(windowed)
    val r = Random(2)
    for _ <- 0 until 30 do
      val xs = input(r); val n = 1 + r.nextInt(6)
      val expected = (0 until n).flatMap(p =>
        Reference.toList(windowed(Strom.fromSeq(xs.toSeq.filter(x => KeyPartitioner.partition(x % 11, n) == p)))))
      assertEquals(run(plan, xs, r, n).sorted, expected.toList.sorted)
  }

  test("empty input") {
    assertEquals(run(Strom.parallelFold(partial), Array.empty, Random(3), 2), List(0))
  }
