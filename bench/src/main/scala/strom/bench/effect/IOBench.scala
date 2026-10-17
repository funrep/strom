package strom.bench.effect

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import strom.effect.StromEffect
import cats.syntax.all.*
import zio.ZIO
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit

/** The same IO code, plain and inside `StromEffect.fused`. Each program loops N times; scores are per
 *  iteration. `lookup` stands for a call the macro can't see into (a repository method, say). */
object Programs:
  final val N = 10000
  def lookup(i: Int): IO[Int] = IO(i ^ 0x5bd1e995)

  // 1. Synchronous loop body: delay + flatMap per iteration.
  def syncLoop: IO[Long] =
    def loop(i: Int, acc: Long): IO[Long] =
      if i == N then IO.pure(acc) else IO.delay(i * 3L).flatMap(x => loop(i + 1, acc + x))
    loop(0, 0L)
  def syncLoopF: IO[Long] = StromEffect.fused {
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
  def forCompF: IO[Long] = StromEffect.fused {
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
  def refLoopF: IO[Int] = StromEffect.fused {
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
  def opaqueOnlyF: IO[Int] = StromEffect.fused {
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
  def errorsF: IO[Int] = StromEffect.fused {
    def loop(i: Int, acc: Int): IO[Int] =
      if i == N then IO.pure(acc)
      else
        IO(if i % 10 == 0 then throw IllegalStateException("bad") else i)
          .handleErrorWith(_ => IO.pure(-1))
          .flatMap(x => loop(i + 1, acc + x))
    loop(0, 0)
  }

  // 6. A straight-line block of six opaque calls with synchronous steps in between. Not a loop: the
  //    block is one effect value, built once, and a plain loop runs it N times.
  val block: IO[Int] =
    for
      a <- lookup(1)
      b <- lookup(a)
      c <- IO(a ^ b)
      d <- lookup(c)
      e <- lookup(d).map(_ & 0xffff)
      f <- lookup(e)
      g <- lookup(f ^ c)
    yield g & 0xff
  val blockF: IO[Int] = StromEffect.fused {
    for
      a <- lookup(1)
      b <- lookup(a)
      c <- IO(a ^ b)
      d <- lookup(c)
      e <- lookup(d).map(_ & 0xffff)
      f <- lookup(e)
      g <- lookup(f ^ c)
    yield g & 0xff
  }
  def repeat(p: IO[Int]): IO[Int] =
    def loop(i: Int, acc: Int): IO[Int] = if i == N then IO.pure(acc) else p.flatMap(x => loop(i + 1, acc + x))
    loop(0, 0)
  def blocks: IO[Int] = repeat(block)
  def blocksF: IO[Int] = repeat(blockF)

  val all: List[(String, () => IO[Any], () => IO[Any])] = List(
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
  @Benchmark def blocksPlain(): Int      = blocks.unsafeRunSync()
  @Benchmark def blocksFused(): Int      = blocksF.unsafeRunSync()

/** Plain and fused must compute the same values. */
@main def check(): Unit =
  for (name, plain, fused) <- Programs.all do
    val (p, f) = (plain().unsafeRunSync(), fused().unsafeRunSync())
    println(s"io $name: $p ${if p == f then "==" else "!="} $f")
    assert(p == f, name)
  for (name, plain, fused) <- CEPrograms.all do
    val (p, f) = (plain().attempt.unsafeRunSync(), fused().attempt.unsafeRunSync())
    println(s"ce $name: $p ${if p == f then "==" else "!="} $f")
    assert(p == f, name)
  for (name, plain, fused) <- ZPrograms.all do
    val (p, f) = (ZPrograms.run(plain()), ZPrograms.run(fused()))
    println(s"zio $name: $p ${if p == f then "==" else "!="} $f")
    assert(p == f, name)
  val shop = List(
    ("io shop seq", ShopIO.seq(false).unsafeRunSync(), ShopIO.seq(true).unsafeRunSync()),
    ("io shop par", ShopIO.par(false).unsafeRunSync(), ShopIO.par(true).unsafeRunSync()),
    ("io shop blocking", ShopIO.seq(false, true).unsafeRunSync(), ShopIO.seq(true, true).unsafeRunSync()),
    ("zio shop seq", ZPrograms.run(ShopZIO.seq(false)), ZPrograms.run(ShopZIO.seq(true))),
    ("zio shop par", ZPrograms.run(ShopZIO.par(false)), ZPrograms.run(ShopZIO.par(true))),
    ("zio shop blocking", ZPrograms.run(ShopZIO.seq(false, true)), ZPrograms.run(ShopZIO.seq(true, true))),
  )
  for (name, p, f) <- shop do
    println(s"$name: $p ${if p == f then "==" else "!="} $f")
    assert(p == f, name)
  // Each order's receipt or rejection, not only the totals.
  def each(c: Services => Checkout) = Services.make(false).flatMap(s => Shop.orders.traverse(o => c(s).checkout(o).attempt)).unsafeRunSync()
  val (ep, ef) = (each(PlainCheckout(_)), each(FusedCheckout(_)))
  assert(ep.map(_.left.map(_.getMessage)) == ef.map(_.left.map(_.getMessage)), "io shop per order")
  def zeach(c: ZServices => ZCheckout) = ZPrograms.run(ZServices.make(false).flatMap(s => ZIO.foreach(Shop.orders)(o => c(s).checkout(o).either)))
  assert(zeach(ZPlainCheckout(_)) == zeach(ZFusedCheckout(_)), "zio shop per order")
  println("shop: per-order results agree")
