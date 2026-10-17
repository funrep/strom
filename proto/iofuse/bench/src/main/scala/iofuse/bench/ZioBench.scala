package iofuse.bench

import iofuse.IOFuse
import org.openjdk.jmh.annotations.*
import zio.{Ref, Runtime, Unsafe, ZIO}
import java.util.concurrent.TimeUnit

/** The IOBench programs written with ZIO. */
object ZPrograms:
  import Programs.N
  type U[A] = ZIO[Any, Nothing, A]
  def lookup(i: Int): U[Int] = ZIO.succeed(i ^ 0x5bd1e995)
  // The leading yield moves the program onto ZIO's executor, as `unsafeRunSync` does for cats-effect:
  // without it ZIO runs the start of the program on the calling (benchmark) thread.
  def run[A](z: ZIO[Any, Any, A]): A =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(ZIO.yieldNow *> z).getOrThrowFiberFailure())

  def syncLoop: U[Long] =
    def loop(i: Int, acc: Long): U[Long] =
      if i == N then ZIO.succeed(acc) else ZIO.succeed(i * 3L).flatMap(x => loop(i + 1, acc + x))
    loop(0, 0L)
  def syncLoopF: U[Long] = IOFuse.fused {
    def loop(i: Int, acc: Long): U[Long] =
      if i == N then ZIO.succeed(acc) else ZIO.succeed(i * 3L).flatMap(x => loop(i + 1, acc + x))
    loop(0, 0L)
  }

  def forComp: U[Long] =
    def loop(i: Int, acc: Long): U[Long] =
      if i == N then ZIO.succeed(acc)
      else
        for
          a <- ZIO.succeed(i)
          b <- ZIO.succeed(a * 7)
          c <- if b % 3 == 0 then ZIO.succeed(b + 1) else ZIO.succeed(b - 1)
          d <- ZIO.succeed(c ^ a).map(_ & 0xffff)
          _ <- ZIO.unit
          r <- loop(i + 1, acc + d)
        yield r
    loop(0, 0L)
  def forCompF: U[Long] = IOFuse.fused {
    def loop(i: Int, acc: Long): U[Long] =
      if i == N then ZIO.succeed(acc)
      else
        for
          a <- ZIO.succeed(i)
          b <- ZIO.succeed(a * 7)
          c <- if b % 3 == 0 then ZIO.succeed(b + 1) else ZIO.succeed(b - 1)
          d <- ZIO.succeed(c ^ a).map(_ & 0xffff)
          _ <- ZIO.unit
          r <- loop(i + 1, acc + d)
        yield r
    loop(0, 0L)
  }

  def refLoop: U[Int] = Ref.make(0).flatMap { ref =>
    def loop(i: Int): U[Int] =
      if i == N then ref.get
      else
        for
          a <- ZIO.succeed(i * 7)
          b <- if a % 3 == 0 then ZIO.succeed(a + 1) else ZIO.succeed(a - 1)
          _ <- ref.update(_ + (b & 0xff))
          r <- loop(i + 1)
        yield r
    loop(0)
  }
  def refLoopF: U[Int] = IOFuse.fused {
    Ref.make(0).flatMap { ref =>
      def loop(i: Int): U[Int] =
        if i == N then ref.get
        else
          for
            a <- ZIO.succeed(i * 7)
            b <- if a % 3 == 0 then ZIO.succeed(a + 1) else ZIO.succeed(a - 1)
            _ <- ref.update(_ + (b & 0xff))
            r <- loop(i + 1)
          yield r
      loop(0)
    }
  }

  def opaqueOnly: U[Int] =
    def loop(i: Int, acc: Int): U[Int] =
      if i == N then ZIO.succeed(acc)
      else lookup(i).flatMap(a => lookup(a)).flatMap(b => loop(i + 1, acc + (b & 0xff)))
    loop(0, 0)
  def opaqueOnlyF: U[Int] = IOFuse.fused {
    def loop(i: Int, acc: Int): U[Int] =
      if i == N then ZIO.succeed(acc)
      else lookup(i).flatMap(a => lookup(a)).flatMap(b => loop(i + 1, acc + (b & 0xff)))
    loop(0, 0)
  }

  // Typed failures (ZIO.fail) every 10th iteration, recovered with catchAll.
  def errors: U[Int] =
    def loop(i: Int, acc: Int): U[Int] =
      if i == N then ZIO.succeed(acc)
      else
        (if i % 10 == 0 then ZIO.fail("bad") else ZIO.succeed(i))
          .catchAll(_ => ZIO.succeed(-1))
          .flatMap(x => loop(i + 1, acc + x))
    loop(0, 0)
  def errorsF: U[Int] = IOFuse.fused {
    def loop(i: Int, acc: Int): U[Int] =
      if i == N then ZIO.succeed(acc)
      else
        (if i % 10 == 0 then ZIO.fail("bad") else ZIO.succeed(i))
          .catchAll(_ => ZIO.succeed(-1))
          .flatMap(x => loop(i + 1, acc + x))
    loop(0, 0)
  }

  // A straight-line block of six opaque calls, run N times by a plain loop.
  val block: U[Int] =
    for
      a <- lookup(1)
      b <- lookup(a)
      c <- ZIO.succeed(a ^ b)
      d <- lookup(c)
      e <- lookup(d).map(_ & 0xffff)
      f <- lookup(e)
      g <- lookup(f ^ c)
    yield g & 0xff
  val blockF: U[Int] = IOFuse.fused {
    for
      a <- lookup(1)
      b <- lookup(a)
      c <- ZIO.succeed(a ^ b)
      d <- lookup(c)
      e <- lookup(d).map(_ & 0xffff)
      f <- lookup(e)
      g <- lookup(f ^ c)
    yield g & 0xff
  }
  def repeat(p: U[Int]): U[Int] =
    def loop(i: Int, acc: Int): U[Int] = if i == N then ZIO.succeed(acc) else p.flatMap(x => loop(i + 1, acc + x))
    loop(0, 0)
  def blocks: U[Int] = repeat(block)
  def blocksF: U[Int] = repeat(blockF)

  val all: List[(String, () => U[Any], () => U[Any])] = List(
    ("blocks", () => blocks, () => blocksF),
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
class ZioBench:
  import ZPrograms.*
  @Benchmark def syncLoopPlain(): Long   = run(syncLoop)
  @Benchmark def syncLoopFused(): Long   = run(syncLoopF)
  @Benchmark def forCompPlain(): Long    = run(forComp)
  @Benchmark def forCompFused(): Long    = run(forCompF)
  @Benchmark def refLoopPlain(): Int     = run(refLoop)
  @Benchmark def refLoopFused(): Int     = run(refLoopF)
  @Benchmark def opaqueOnlyPlain(): Int  = run(opaqueOnly)
  @Benchmark def opaqueOnlyFused(): Int  = run(opaqueOnlyF)
  @Benchmark def errorsPlain(): Int      = run(errors)
  @Benchmark def errorsFused(): Int      = run(errorsF)
  @Benchmark def blocksPlain(): Int      = run(blocks)
  @Benchmark def blocksFused(): Int      = run(blocksF)
