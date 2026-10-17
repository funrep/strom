package strom.bench.effect

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import strom.effect.StromEffect
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit

/** Programs from cats-effect's own benchmarks (benchmarks/src/main/scala/cats/effect/benchmarks at
 *  v3.7.1, Apache License 2.0, Copyright 2020-2025 Typelevel), copied as written, each plain and
 *  inside `fused`: code we did not write for the fuser. Left out: MapCalls (its `map` chain is
 *  built at run time in a `while` loop, nothing a macro sees), and the concurrency and data
 *  structure benchmarks (Queue, Deferred, Mutex, ...), whose code is in the library. */
object CEPrograms:
  final val size = 10000
  val dummy = new RuntimeException("dummy")

  // DeepBindBenchmark
  def deepPure: IO[Int] =
    def loop(i: Int): IO[Int] = IO.pure(i).flatMap { j => if (j > size) IO.pure(j) else loop(j + 1) }
    loop(0)
  def deepPureF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] = IO.pure(i).flatMap { j => if (j > size) IO.pure(j) else loop(j + 1) }
    loop(0)
  }
  def deepDelay: IO[Int] =
    def loop(i: Int): IO[Int] = IO(i).flatMap { j => if (j > size) IO.pure(j) else loop(j + 1) }
    loop(0)
  def deepDelayF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] = IO(i).flatMap { j => if (j > size) IO.pure(j) else loop(j + 1) }
    loop(0)
  }
  def deepAsync: IO[Int] =
    def loop(i: Int): IO[Int] = IO(i).flatMap { j => IO.cede.flatMap { _ => if (j > size) IO.pure(j) else loop(j + 1) } }
    loop(0)
  def deepAsyncF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] = IO(i).flatMap { j => IO.cede.flatMap { _ => if (j > size) IO.pure(j) else loop(j + 1) } }
    loop(0)
  }

  // ShallowBindBenchmark
  def shallowPure: IO[Int] =
    def loop(i: Int): IO[Int] = if (i < size) IO.pure(i + 1).flatMap(loop) else IO.pure(i)
    IO.pure(0).flatMap(loop)
  def shallowPureF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] = if (i < size) IO.pure(i + 1).flatMap(loop) else IO.pure(i)
    IO.pure(0).flatMap(loop)
  }
  def shallowDelay: IO[Int] =
    def loop(i: Int): IO[Int] = if (i < size) IO(i + 1).flatMap(loop) else IO(i)
    IO(0).flatMap(loop)
  def shallowDelayF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] = if (i < size) IO(i + 1).flatMap(loop) else IO(i)
    IO(0).flatMap(loop)
  }
  def shallowAsync: IO[Int] =
    def loop(i: Int): IO[Int] =
      if (i < size) IO.cede.flatMap(_ => IO.pure(i + 1)).flatMap(loop) else IO.cede.flatMap(_ => IO.pure(i))
    IO(0).flatMap(loop)
  def shallowAsyncF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] =
      if (i < size) IO.cede.flatMap(_ => IO.pure(i + 1)).flatMap(loop) else IO.cede.flatMap(_ => IO.pure(i))
    IO(0).flatMap(loop)
  }

  // AttemptBenchmark
  def attemptHappy: IO[Int] =
    def loop(i: Int): IO[Int] = if (i < size) IO.pure(i + 1).attempt.flatMap(_.fold(IO.raiseError, loop)) else IO.pure(i)
    loop(0)
  def attemptHappyF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] = if (i < size) IO.pure(i + 1).attempt.flatMap(_.fold(IO.raiseError, loop)) else IO.pure(i)
    loop(0)
  }
  def attemptError: IO[Int] =
    val id = IO.pure[Int] _
    def loop(i: Int): IO[Int] =
      if (i < size) IO.raiseError[Int](dummy).flatMap(x => IO.pure(x + 1)).attempt.flatMap(_.fold(_ => loop(i + 1), id))
      else IO.pure(i)
    loop(0)
  def attemptErrorF: IO[Int] = StromEffect.fused {
    val id = IO.pure[Int] _
    def loop(i: Int): IO[Int] =
      if (i < size) IO.raiseError[Int](dummy).flatMap(x => IO.pure(x + 1)).attempt.flatMap(_.fold(_ => loop(i + 1), id))
      else IO.pure(i)
    loop(0)
  }

  // HandleErrorBenchmark
  def handleHappy: IO[Int] =
    def loop(i: Int): IO[Int] = if (i < size) IO.pure(i + 1).handleErrorWith(IO.raiseError).flatMap(loop) else IO.pure(i)
    loop(0)
  def handleHappyF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] = if (i < size) IO.pure(i + 1).handleErrorWith(IO.raiseError).flatMap(loop) else IO.pure(i)
    loop(0)
  }
  def handleError: IO[Int] =
    def loop(i: Int): IO[Int] =
      if (i < size) IO.raiseError[Int](dummy).flatMap(x => IO.pure(x + 1)).flatMap(x => IO.pure(x + 1)).handleErrorWith(_ => loop(i + 1))
      else IO.pure(i)
    loop(0)
  def handleErrorF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] =
      if (i < size) IO.raiseError[Int](dummy).flatMap(x => IO.pure(x + 1)).flatMap(x => IO.pure(x + 1)).handleErrorWith(_ => loop(i + 1))
      else IO.pure(i)
    loop(0)
  }

  // RefBenchmark
  def refModify: IO[Long] =
    Ref[IO].of(0L).flatMap { ref =>
      def loop(remaining: Int, acc: Long): IO[Long] =
        if (remaining == 0) IO(acc) else ref.modify(n => (n + 1, n)).flatMap(prev => loop(remaining - 1, acc + prev))
      loop(size, 0L)
    }
  def refModifyF: IO[Long] = StromEffect.fused {
    Ref[IO].of(0L).flatMap { ref =>
      def loop(remaining: Int, acc: Long): IO[Long] =
        if (remaining == 0) IO(acc) else ref.modify(n => (n + 1, n)).flatMap(prev => loop(remaining - 1, acc + prev))
      loop(size, 0L)
    }
  }
  def refGetAndUpdate: IO[Long] =
    Ref[IO].of(0L).flatMap { ref =>
      def loop(remaining: Int, acc: Long): IO[Long] =
        if (remaining == 0) IO(acc) else ref.getAndUpdate(_ + 1).flatMap(prev => loop(remaining - 1, acc + prev))
      loop(size, 0L)
    }
  def refGetAndUpdateF: IO[Long] = StromEffect.fused {
    Ref[IO].of(0L).flatMap { ref =>
      def loop(remaining: Int, acc: Long): IO[Long] =
        if (remaining == 0) IO(acc) else ref.getAndUpdate(_ + 1).flatMap(prev => loop(remaining - 1, acc + prev))
      loop(size, 0L)
    }
  }

  // BothBenchmark
  def both: IO[Int] =
    def loop(i: Int): IO[Int] = if (i < size) IO.both(IO.pure(i + 1), IO.pure(i + 1)).flatMap(p => loop(p._1)) else IO.pure(i)
    loop(0)
  def bothF: IO[Int] = StromEffect.fused {
    def loop(i: Int): IO[Int] = if (i < size) IO.both(IO.pure(i + 1), IO.pure(i + 1)).flatMap(p => loop(p._1)) else IO.pure(i)
    loop(0)
  }

  val all: List[(String, () => IO[Any], () => IO[Any])] = List(
    ("deepPure", () => deepPure, () => deepPureF), ("deepDelay", () => deepDelay, () => deepDelayF),
    ("deepAsync", () => deepAsync, () => deepAsyncF), ("shallowPure", () => shallowPure, () => shallowPureF),
    ("shallowDelay", () => shallowDelay, () => shallowDelayF), ("shallowAsync", () => shallowAsync, () => shallowAsyncF),
    ("attemptHappy", () => attemptHappy, () => attemptHappyF), ("attemptError", () => attemptError, () => attemptErrorF),
    ("handleHappy", () => handleHappy, () => handleHappyF), ("handleError", () => handleError, () => handleErrorF),
    ("refModify", () => refModify, () => refModifyF), ("refGetAndUpdate", () => refGetAndUpdate, () => refGetAndUpdateF),
    ("both", () => both, () => bothF),
  )

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = Array("-Xms1g", "-Xmx1g"))
@OperationsPerInvocation(CEPrograms.size)
class CEBench:
  import CEPrograms.*
  @Benchmark def deepPurePlain(): Int          = deepPure.unsafeRunSync()
  @Benchmark def deepPureFused(): Int          = deepPureF.unsafeRunSync()
  @Benchmark def deepDelayPlain(): Int         = deepDelay.unsafeRunSync()
  @Benchmark def deepDelayFused(): Int         = deepDelayF.unsafeRunSync()
  @Benchmark def deepAsyncPlain(): Int         = deepAsync.unsafeRunSync()
  @Benchmark def deepAsyncFused(): Int         = deepAsyncF.unsafeRunSync()
  @Benchmark def shallowPurePlain(): Int       = shallowPure.unsafeRunSync()
  @Benchmark def shallowPureFused(): Int       = shallowPureF.unsafeRunSync()
  @Benchmark def shallowDelayPlain(): Int      = shallowDelay.unsafeRunSync()
  @Benchmark def shallowDelayFused(): Int      = shallowDelayF.unsafeRunSync()
  @Benchmark def shallowAsyncPlain(): Int      = shallowAsync.unsafeRunSync()
  @Benchmark def shallowAsyncFused(): Int      = shallowAsyncF.unsafeRunSync()
  @Benchmark def attemptHappyPlain(): Int      = attemptHappy.unsafeRunSync()
  @Benchmark def attemptHappyFused(): Int      = attemptHappyF.unsafeRunSync()
  @Benchmark def attemptErrorPlain(): Int      = attemptError.unsafeRunSync()
  @Benchmark def attemptErrorFused(): Int      = attemptErrorF.unsafeRunSync()
  @Benchmark def handleHappyPlain(): Int       = handleHappy.unsafeRunSync()
  @Benchmark def handleHappyFused(): Int       = handleHappyF.unsafeRunSync()
  @Benchmark def handleErrorPlain(): Int       = handleError.unsafeRunSync()
  @Benchmark def handleErrorFused(): Int       = handleErrorF.unsafeRunSync()
  @Benchmark def refModifyPlain(): Long        = refModify.unsafeRunSync()
  @Benchmark def refModifyFused(): Long        = refModifyF.unsafeRunSync()
  @Benchmark def refGetAndUpdatePlain(): Long  = refGetAndUpdate.unsafeRunSync()
  @Benchmark def refGetAndUpdateFused(): Long  = refGetAndUpdateF.unsafeRunSync()
  @Benchmark def bothPlain(): Int              = both.unsafeRunSync()
  @Benchmark def bothFused(): Int              = bothF.unsafeRunSync()
