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
 *  - fused segments that `cede` once per time slice (Ström's default: 100 µs),
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

  /** Sleeps 1 ms in a loop for `duration`; returns how late each wake-up was (ms). All timing is
   *  taken inside IO, when the ticker actually runs. */
  def ticker(duration: FiniteDuration): IO[Vector[Double]] =
    def loop(until: Long, acc: Vector[Double]): IO[Vector[Double]] =
      IO.monotonic.flatMap { t0 =>
        if t0.toNanos >= until then IO.pure(acc)
        else IO.sleep(1.milli) >> IO.monotonic.flatMap(t1 => loop(until, acc :+ ((t1 - t0).toNanos / 1e6 - 1.0)))
      }
    IO.monotonic.flatMap(start => loop((start + duration).toNanos, Vector.empty))

  def measure(name: String, load: IO[Long]): IO[Unit] =
    val loads = List.fill(cores)(load.foreverM.void.timeout(4.seconds).attempt.void).parSequence_
    (ticker(3.seconds), loads).parTupled.flatMap { (lates, _) =>
      val s = lates.sorted
      def pct(p: Double) = if s.isEmpty then Double.NaN else s(((s.size - 1) * p).toInt)
      IO.println(f"$name%-34s ticks=${s.size}%5d  p50=${pct(0.5)}%8.3f ms  p99=${pct(0.99)}%8.3f ms  max=${s.lastOption.getOrElse(Double.NaN)}%8.3f ms")
    }

  def run(args: List[String]): IO[ExitCode] =
    val pool = Executors.newFixedThreadPool(cores)
    val compute = ExecutionContext.fromExecutor(pool)
    (sweepOrFull(args, pool, compute)).guarantee(IO(pool.shutdownNow()).void)

  /** Loads the machine for a few seconds first so that the measurements see JIT-compiled code. */
  val warmUp: IO[Unit] =
    IO.println("warming up…") >> List.fill(cores)(segmentLoad(4096).foreverM.void.timeout(5.seconds).attempt.void).parSequence_

  def sweepOrFull(args: List[String], pool: java.util.concurrent.ExecutorService, compute: ExecutionContext): IO[ExitCode] =
    warmUp >> measureAll(args, pool, compute)

  def measureAll(args: List[String], pool: java.util.concurrent.ExecutorService, compute: ExecutionContext): IO[ExitCode] =
    val budgets = args.headOption.map(_.split(',').toList.map(_.toInt))
    budgets match
      case Some(bs) => // budget sweep: `bench/runMain strom.bench.Latency 256,1024,4096`
        bs.traverse_(b => measure(s"segment, budget $b", segmentLoad(b))).as(ExitCode.Success)
      case None => full(pool, compute)

  def full(pool: java.util.concurrent.ExecutorService, compute: ExecutionContext): IO[ExitCode] =
    for
      _ <- IO.println(s"cores=$cores, ${cores} concurrent loads, ticker sleeps 1 ms")
      _ <- measure("idle (no load)", IO.never)
      _ <- measure("segment (cede every 100 µs slice)", segmentLoad(strom.runtime.Segment.DefaultBudget))
      _ <- measure("segment, budget Int.MaxValue (no yield)", segmentLoad(Int.MaxValue))
      _ <- measure("direct loop in IO.delay", directLoad)
      _ <- measure("segment on dedicated pool", segmentLoad(strom.runtime.Segment.DefaultBudget).evalOn(compute))
    yield ExitCode.Success
