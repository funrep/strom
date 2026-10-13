package iofuse.bench

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import iofuse.IOFuse
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit

/** The same IO code, plain and inside `IOFuse.fused`. Each program loops N times; scores are per
 *  iteration. `lookup` stands for a call the macro can't see into (a repository method, say). */
object Programs:
  final val N = 10000
  def lookup(i: Int): IO[Int] = IO(i ^ 0x5bd1e995)

  // 1. Synchronous loop body: delay + flatMap per iteration.
  def syncLoop: IO[Long] =
    def loop(i: Int, acc: Long): IO[Long] =
      if i == N then IO.pure(acc) else IO.delay(i * 3L).flatMap(x => loop(i + 1, acc + x))
    loop(0, 0L)
  def syncLoopF: IO[Long] = IOFuse.fused {
    def loop(i: Int, acc: Long): IO[Long] =
      if i == N then IO.pure(acc) else IO.delay(i * 3L).flatMap(x => loop(i + 1, acc + x))
    loop(0, 0L)
  }

  // 2. A six-step synchronous for-comprehension per iteration (validation / mapping code).
  def forComp: IO[Long] =
    def loop(i: Int, acc: Long): IO[Long] =
      if i == N then IO.pure(acc)
      else
        for
          a <- IO.pure(i)
          b <- IO(a * 7)
          c <- if b % 3 == 0 then IO.pure(b + 1) else IO(b - 1)
          d <- IO(c ^ a).map(_ & 0xffff)
          _ <- IO.unit
          r <- loop(i + 1, acc + d)
        yield r
    loop(0, 0L)
  def forCompF: IO[Long] = IOFuse.fused {
    def loop(i: Int, acc: Long): IO[Long] =
      if i == N then IO.pure(acc)
      else
        for
          a <- IO.pure(i)
          b <- IO(a * 7)
          c <- if b % 3 == 0 then IO.pure(b + 1) else IO(b - 1)
          d <- IO(c ^ a).map(_ & 0xffff)
          _ <- IO.unit
          r <- loop(i + 1, acc + d)
        yield r
    loop(0, 0L)
  }

  // 3. Same sync steps plus one opaque Ref update per iteration.
  def refLoop: IO[Int] = Ref[IO].of(0).flatMap { ref =>
    def loop(i: Int): IO[Int] =
      if i == N then ref.get
      else
        for
          a <- IO(i * 7)
          b <- if a % 3 == 0 then IO.pure(a + 1) else IO(a - 1)
          _ <- ref.update(_ + (b & 0xff))
          r <- loop(i + 1)
        yield r
    loop(0)
  }
  def refLoopF: IO[Int] = IOFuse.fused {
    Ref[IO].of(0).flatMap { ref =>
      def loop(i: Int): IO[Int] =
        if i == N then ref.get
        else
          for
            a <- IO(i * 7)
            b <- if a % 3 == 0 then IO.pure(a + 1) else IO(a - 1)
            _ <- ref.update(_ + (b & 0xff))
            r <- loop(i + 1)
          yield r
      loop(0)
    }
  }

  // 4. Only opaque calls: lookup → lookup → loop. Fusing saves only the glue.
  def opaqueOnly: IO[Int] =
    def loop(i: Int, acc: Int): IO[Int] =
      if i == N then IO.pure(acc)
      else lookup(i).flatMap(a => lookup(a)).flatMap(b => loop(i + 1, acc + (b & 0xff)))
    loop(0, 0)
  def opaqueOnlyF: IO[Int] = IOFuse.fused {
    def loop(i: Int, acc: Int): IO[Int] =
      if i == N then IO.pure(acc)
      else lookup(i).flatMap(a => lookup(a)).flatMap(b => loop(i + 1, acc + (b & 0xff)))
    loop(0, 0)
  }

  // 5. Error handling: a sync step that fails every 10th iteration, recovered.
  def errors: IO[Int] =
    def loop(i: Int, acc: Int): IO[Int] =
      if i == N then IO.pure(acc)
      else
        IO(if i % 10 == 0 then throw IllegalStateException("bad") else i)
          .handleErrorWith(_ => IO.pure(-1))
          .flatMap(x => loop(i + 1, acc + x))
    loop(0, 0)
  def errorsF: IO[Int] = IOFuse.fused {
    def loop(i: Int, acc: Int): IO[Int] =
      if i == N then IO.pure(acc)
      else
        IO(if i % 10 == 0 then throw IllegalStateException("bad") else i)
          .handleErrorWith(_ => IO.pure(-1))
          .flatMap(x => loop(i + 1, acc + x))
    loop(0, 0)
  }

  val all: List[(String, () => IO[Any], () => IO[Any])] = List(
    ("syncLoop", () => syncLoop, () => syncLoopF),
    ("forComp", () => forComp, () => forCompF),
    ("refLoop", () => refLoop, () => refLoopF),
    ("opaqueOnly", () => opaqueOnly, () => opaqueOnlyF),
    ("errors", () => errors, () => errorsF),
  )

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = Array("-Xms1g", "-Xmx1g"))
@OperationsPerInvocation(Programs.N)
class IOBench:
  import Programs.*
  @Benchmark def syncLoopPlain(): Long   = syncLoop.unsafeRunSync()
  @Benchmark def syncLoopFused(): Long   = syncLoopF.unsafeRunSync()
  @Benchmark def forCompPlain(): Long    = forComp.unsafeRunSync()
  @Benchmark def forCompFused(): Long    = forCompF.unsafeRunSync()
  @Benchmark def refLoopPlain(): Int     = refLoop.unsafeRunSync()
  @Benchmark def refLoopFused(): Int     = refLoopF.unsafeRunSync()
  @Benchmark def opaqueOnlyPlain(): Int  = opaqueOnly.unsafeRunSync()
  @Benchmark def opaqueOnlyFused(): Int  = opaqueOnlyF.unsafeRunSync()
  @Benchmark def errorsPlain(): Int      = errors.unsafeRunSync()
  @Benchmark def errorsFused(): Int      = errorsF.unsafeRunSync()

/** Plain and fused must compute the same values. */
@main def check(): Unit =
  for (name, plain, fused) <- Programs.all do
    val (p, f) = (plain().unsafeRunSync(), fused().unsafeRunSync())
    println(s"$name: $p ${if p == f then "==" else "!="} $f")
    assert(p == f, name)
