package example

import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import fs2.{Stream, text}
import strom.fs2.StromFs2
import strom.zio.StromZio
import zio.{Chunk, Runtime, Unsafe, ZIO}
import zio.stream.ZStream

/** Pipelines in the style of real fs2 / ZIO code. For each: what `fused` compiles, and a check
 *  that the fused and plain versions agree. */
object Realistic extends IOApp.Simple:
  final case class Req(status: Int, latencyMs: Long, path: String)
  def parse(line: String): Req =
    val p = line.split(' ')
    Req(p(0).toInt, p(1).toLong, p(2))

  val logLines: List[String] = List.tabulate(5000)(i => s"${if i % 13 == 0 then 503 else 200} ${i % 97} /api/v${i % 3}")
  val csv: String = List.tabulate(3000)(i => s"sensor$i,${i % 17},${(i * 37) % 1000 / 10.0}").mkString("\n")

  // 1. Log processing: decode → lines → parse → filter → aggregate.
  def logs = Stream.emits(logLines).covary[IO]
  inline def p1 = logs.map(parse).filter(_.status >= 500).map(_.latencyMs).fold(0L)(_ + _)

  // 2. CSV metrics from bytes, with the usual text pipes and option-returning parsing.
  def bytes = Stream.emit(csv).covary[IO].through(text.utf8.encode)
  inline def p2 = bytes.through(text.utf8.decode).through(text.lines)
    .map(_.split(',')).filter(_.length == 3).mapFilter(a => a(2).toDoubleOption).filter(_ > 50.0).fold(0.0)(_ + _)

  // 3. Indexing, de-duplication by window, prefix sums.
  def ints = Stream.range(0, 20000).covary[IO]
  inline def p3 = ints.map(_ * 7 % 1000).zipWithIndex.collect { case (v, i) if i % 2 == 0 => v }
    .scan(0)(_ + _).drop(10).takeWhile(_ < 3000000).map(_ / 3)

  // 4. Expansion with an effect in the middle.
  inline def p4 = ints.filter(_ % 5 == 0).flatMap(x => Stream.range(0, x % 4).map(_ + x))
    .evalMap(x => IO.pure(x)).map(_ * 2).filter(_ % 3 != 0).take(500L)

  // 5. ZIO: similar shapes.
  def zints = ZStream.range(0, 20000)
  inline def z1 = zints.map(_ * 3).filter(_ % 2 == 0).mapZIO(x => ZIO.succeed(x)).map(_ + 1).take(100)
  inline def z2 = zints.map(_ % 100).collect { case x if x > 10 => x * 2 }.zipWithIndex.map(_._1).filter(_ != 40)
  inline def z3 = zints.flatMap(x => ZStream.range(0, x % 3)).map(_ * 2).zipWith(zints.map(_ + 1))(_ + _).take(300)

  def zrun[A](s: ZStream[Any, Nothing, A]): List[A] =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(s.runCollect).getOrThrow()).toList

  def show(name: String, plan: String, same: Boolean) =
    IO.println(s"── $name ${if same then "✓ fused = plain" else "✗ MISMATCH"}\n${if plan.isEmpty then "(nothing fused)" else plan}")

  def check[A](name: String, plan: String, plain: Stream[IO, A], fused: Stream[IO, A]): IO[Unit] =
    (plain.compile.toList, fused.compile.toList).flatMapN((a, b) => show(name, plan, a == b))

  def run: IO[Unit] =
    check("fs2 logs", StromFs2.fusedPlan(p1), p1, StromFs2.fused(p1)) >>
    check("fs2 csv", StromFs2.fusedPlan(p2), p2, StromFs2.fused(p2)) >>
    check("fs2 index/scan", StromFs2.fusedPlan(p3), p3, StromFs2.fused(p3)) >>
    check("fs2 expand+effect", StromFs2.fusedPlan(p4), p4, StromFs2.fused(p4)) >>
    show("zio map/effect", StromZio.fusedPlan(z1), zrun(z1) == zrun(StromZio.fused(z1))) >>
    show("zio collect/index", StromZio.fusedPlan(z2), zrun(z2) == zrun(StromZio.fused(z2))) >>
    show("zio expand+zip", StromZio.fusedPlan(z3), zrun(z3) == zrun(StromZio.fused(z3)))
