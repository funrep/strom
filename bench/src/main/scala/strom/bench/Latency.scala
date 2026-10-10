package strom.bench

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import cats.effect.{IO, IOApp, ExitCode}
import cats.syntax.all.*
import strom.Strom
import strom.fs2.StromFs2

/** I/O latency under CPU load: while every compute thread runs a long fused pipeline, a ticker
 *  fiber sleeps 1 ms in a loop and records how late it wakes up. Compares
 *  - fused segments that `cede` every `budget` steps (Ström's default),
 *  - the same with an effectively infinite budget (no yields inside a chunk),
 *  - a direct fused loop inside one `IO.delay` (never yields),
 *  - fused segments on a dedicated compute pool (`evalOn`), leaving the runtime's threads free. */
object Latency extends IOApp:
  val cores = Runtime.getRuntime.availableProcessors()
  val input: Array[Int] = Array.fill(64)(2_000_000) // each element expands to 2M inner elements

  val seg = Strom.foldSegment((a: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x)).map(y => (y * 31) ^ (y >>> 3)).filter(_ % 7 != 0).count)

  def segmentLoad(budget: Int): IO[Long] =
    _root_.fs2.Stream.emits(input.toSeq).covary[IO].chunkLimit(1).unchunks
      .through(StromFs2.fold[IO, Int, Long](seg, budget)).compile.lastOrError

  def directLoad: IO[Long] = IO.delay(Strom.run(
    Strom.fromArray(input).flatMap(x => Strom.range(0, x)).map(y => (y * 31) ^ (y >>> 3)).filter(_ % 7 != 0).count))

  def ticker(until: Deadline): IO[Vector[Double]] =
    def loop(acc: Vector[Double]): IO[Vector[Double]] =
      if until.isOverdue() then IO.pure(acc)
      else
        val t0 = System.nanoTime()
        IO.sleep(1.milli) >> IO.delay((System.nanoTime() - t0) / 1e6 - 1.0).flatMap(late => loop(acc :+ late))
    loop(Vector.empty)

  def measure(name: String, load: IO[Long]): IO[Unit] =
    val deadline = 3.seconds.fromNow
    val loads = List.fill(cores)(load.foreverM.void.timeout(4.seconds).attempt.void).parSequence_
    (ticker(deadline), loads).parTupled.flatMap { (lates, _) =>
      val s = lates.sorted
      def pct(p: Double) = if s.isEmpty then Double.NaN else s(((s.size - 1) * p).toInt)
      IO.println(f"$name%-34s ticks=${s.size}%5d  p50=${pct(0.5)}%8.3f ms  p99=${pct(0.99)}%8.3f ms  max=${s.lastOption.getOrElse(Double.NaN)}%8.3f ms")
    }

  def run(args: List[String]): IO[ExitCode] =
    val pool = Executors.newFixedThreadPool(cores)
    val compute = ExecutionContext.fromExecutor(pool)
    for
      _ <- IO.println(s"cores=$cores, ${cores} concurrent loads, ticker sleeps 1 ms")
      _ <- measure("idle (no load)", IO.never)
      _ <- measure("segment, budget 4096 (cede)", segmentLoad(4096))
      _ <- measure("segment, budget Int.MaxValue", segmentLoad(Int.MaxValue))
      _ <- measure("direct loop in IO.delay", directLoad)
      _ <- measure("segment on dedicated pool", segmentLoad(4096).evalOn(compute))
      _ <- IO(pool.shutdownNow())
    yield ExitCode.Success
