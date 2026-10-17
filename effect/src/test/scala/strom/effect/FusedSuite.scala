package strom.effect

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import cats.effect.unsafe.IORuntime
import java.util.concurrent.{Executors, TimeoutException}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** Differential tests: each program runs plain and fused; result (or error) and the effect trace
 *  must be identical. */
class FusedSuite extends munit.FunSuite:
  final class Boom(msg: String) extends RuntimeException(msg)

  def outcome[A](io: IO[A]): Either[String, A] =
    io.attempt.unsafeRunSync().left.map(e => s"${e.getClass.getSimpleName}: ${e.getMessage}")

  inline def same[A](inline prog: ListBuffer[String] => IO[A])(using munit.Location): A =
    val pl = ListBuffer.empty[String]
    val fl = ListBuffer.empty[String]
    val p = outcome(prog(pl))
    val f = outcome(StromEffect.fused(prog(fl)))
    assertEquals(f, p)
    assertEquals(fl.toList, pl.toList)
    p.getOrElse(null.asInstanceOf[A])

  def opaque(log: ListBuffer[String], s: String): IO[Unit] = IO(log += s).void
  def lookup(i: Int): IO[Int] = IO(i * 10)

  test("straight-line for-comprehension") {
    val r = same(log =>
      for
        a <- IO.pure(3)
        b <- IO(log += s"b $a").as(a * 2)
        _ <- IO.unit
        c <- IO.delay { log += "c"; b + 1 }.map(_ * 3)
        d = c + a
        e <- if d > 10 then IO(log += "big").as(d) else IO.pure(-d)
      yield e + b)
    assertEquals(r, 30)
  }

  test("opaque calls in between, product operators") {
    same(log =>
      for
        a <- IO.pure(1)
        _ <- opaque(log, s"one $a")
        b <- lookup(a) <* IO(log += "after lookup")
        c <- IO(log += "x") *> lookup(b) >> IO.pure(b + 1)
        _ <- opaque(log, "two").void
      yield (a, b, c))
  }

  test("match with opaque and sync branches; join point") {
    for i <- 0 until 4 do
      same(log =>
        IO.pure(i).flatMap {
          case 0 => IO(log += "zero").as(0)
          case 1 => lookup(1).map(_ + 1)
          case n if n % 2 == 0 => opaque(log, s"even $n").as(n)
          case n => IO.pure(n)
        }.flatMap(x => IO(log += s"after $x").as(x * 2)))
  }

  test("sync loop: one million iterations, stack safe") {
    val r = same(log =>
      def loop(i: Int, acc: Long): IO[Long] =
        if i >= 1000000 then IO.pure(acc)
        else IO.delay(i * 3L).flatMap(x => loop(i + 1, acc + x))
      loop(0, 0L))
    assertEquals(r, 3L * 999999L * 1000000L / 2)
  }

  test("loop with an opaque Ref every iteration") {
    same(log =>
      Ref[IO].of(0).flatMap { ref =>
        def loop(i: Int): IO[Int] =
          if i == 5000 then ref.get
          else
            for
              x <- IO(i % 7)
              _ <- ref.update(_ + x)
              _ <- if i % 1000 == 0 then IO(log += s"at $i") else IO.unit
              r <- loop(i + 1)
            yield r
        loop(0)
      })
  }

  test("loop with a branch that is opaque only sometimes") {
    same(log =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 3000 then IO.pure(acc)
        else
          val step = if i % 100 == 0 then lookup(i) else IO.pure(i)
          step.flatMap(x => loop(i + 1, acc ^ x))
      loop(0, 0))
  }

  test("non-tail recursion and mutual recursion are stack safe") {
    // Plain IO needs IO.defer here (the recursion would run while building the IO).
    same(_ =>
      def down(n: Int): IO[Int] = if n == 0 then IO.pure(0) else IO.defer(down(n - 1)).map(_ + 1)
      down(100000))
    same(_ =>
      def even(n: Int): IO[Boolean] = if n == 0 then IO.pure(true) else IO.defer(odd(n - 1))
      def odd(n: Int): IO[Boolean] = if n == 0 then IO.pure(false) else IO.defer(even(n - 1))
      even(100001))
  }

  test("errors: raiseError, throwing delay, throwing map") {
    same(log => IO(log += "a") *> IO.raiseError[Int](Boom("raised")) <* IO(log += "never"))
    same(log => IO(log += "a") *> IO[Int](throw Boom("delay")).map(_ + 1))
    same(log => IO.pure(1).map(x => if x == 1 then throw Boom("map") else x) <* IO(log += "never"))
    same(log => lookup(1).flatMap(x => IO(log += "a") *> IO.raiseError[Int](Boom(s"after $x"))))
  }

  test("errors: handleErrorWith and attempt around sync and opaque regions") {
    same(log =>
      (IO(log += "a") *> IO.raiseError[Int](Boom("x"))).handleErrorWith(e => IO(log += e.getMessage).as(-1)))
    same(log =>
      (lookup(2) *> IO.raiseError[Int](Boom("y"))).handleErrorWith(e => opaque(log, e.getMessage).as(-2)))
    same(log => (IO(log += "b") *> IO[Int](throw Boom("z"))).attempt.map(_.left.map(_.getMessage)))
    same(log => (lookup(3).flatMap(_ => IO.raiseError[Int](Boom("w")))).attempt.map(_.isLeft))
    // An error after the handled region is not caught by it.
    same(log => IO.pure(1).handleErrorWith(_ => IO.pure(2)).flatMap(_ => IO.raiseError[Int](Boom("later"))))
    // Handler with a non-literal function.
    val h: Throwable => IO[Int] = e => IO.pure(e.getMessage.length)
    same(log => IO.raiseError[Int](Boom("abc")).handleErrorWith(h))
  }

  test("errors inside loops") {
    same(log =>
      def loop(i: Int): IO[Int] =
        if i == 2000 then IO.pure(i)
        else
          IO(if i % 500 == 499 then throw Boom(s"at $i") else i)
            .handleErrorWith(e => IO(log += e.getMessage).as(0))
            .flatMap(_ => loop(i + 1))
      loop(0))
    same(log =>
      def loop(i: Int): IO[Int] =
        if i == 2000 then IO.pure(i) else IO(i).flatMap(x => if x == 1234 then IO.raiseError(Boom("stop")) else loop(i + 1))
      loop(0))
  }

  test("a local def used as a value stays lazy; reusing its IO reruns it") {
    same(log =>
      def loop(i: Int): IO[Int] = if i == 3 then IO.pure(i) else IO(log += s"step $i").flatMap(_ => loop(i + 1))
      val x = loop(0)
      IO(log += "start") *> x *> x)
  }

  test("non-literal functions") {
    val f: Int => IO[Int] = x => IO.pure(x + 1)
    val g: Int => Int = _ * 2
    same(log => IO(1).flatMap(f).map(g).flatMap(f))
  }

  test("a fused IO can run many times, also concurrently") {
    val prog = StromEffect.fused {
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 10000 then IO.pure(acc) else lookup(i % 3).flatMap(x => loop(i + 1, acc + x))
      loop(0, 0)
    }
    val expected = prog.unsafeRunSync()
    assertEquals(prog.unsafeRunSync(), expected)
    assertEquals(IO.parReplicateAN(8)(32, prog).unsafeRunSync().distinct, List(expected))
  }

  test("show: the loop is a while, nothing to fuse leaves the code alone") {
    val code = StromEffect.show {
      def loop(i: Int): IO[Int] = if i == 0 then IO.pure(0) else IO(i - 1).flatMap(loop)
      loop(10)
    }
    assert(code.contains("while"), code)
    assertEquals(StromEffect.show(lookup(1)), "FusedSuite.this.lookup(1)")
  }

  test("endless compiled loops cede: fair and cancelable on one compute thread") {
    val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
    val rt = IORuntime.builder().setCompute(ec, () => ec.shutdown()).build()
    def spin(c: AtomicLong): IO[Unit] = StromEffect.fused {
      def loop(i: Long): IO[Unit] = IO(c.incrementAndGet()).flatMap(_ => loop(i + 1))
      loop(0)
    }
    val (a, b) = (AtomicLong(), AtomicLong())
    try
      val r = IO.race(spin(a), spin(b)).timeout(300.millis).attempt.unsafeRunSync()(using rt)
      assert(r.left.exists(_.isInstanceOf[TimeoutException]), r)
      assert(a.get > 0 && b.get > 0, (a.get, b.get))
    finally rt.shutdown()
  }

  test("a synchronous part with a handler that is not: recover by looping, fail in the handler, suspend") {
    // The handler calls the loop: compiled as try/catch and a jump. Deep, so a nested handler would show.
    assertEquals(same(log =>
      def loop(i: Int): IO[Int] =
        if i < 200000 then IO.raiseError[Int](Boom("x")).map(_ + 1).handleErrorWith(_ => loop(i + 1))
        else IO.pure(i)
      loop(0)), 200000)
    // Only some iterations fail; the value of the others goes on.
    same(log =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 3000 then IO.pure(acc)
        else IO(if i % 7 == 0 then throw Boom(s"b$i") else i)
          .handleErrorWith(e => opaque(log, e.getMessage).as(-1))
          .flatMap(x => loop(i + 1, acc + x))
      loop(0, 0))
    // An error in the handler is not caught by the same handler, but by the one outside.
    assertEquals(same(log =>
      (IO(log += "a") *> IO.raiseError[Int](Boom("first")))
        .handleErrorWith(e => opaque(log, e.getMessage) *> IO.raiseError[Int](Boom("second")))
        .handleErrorWith(e => IO(log += s"outer ${e.getMessage}").as(7))), 7)
    // A handler that suspends, in a for-comprehension with more after it.
    same(log =>
      for
        a <- lookup(1)
        b <- IO(if a > 5 then throw Boom("big") else a).handleErrorWith(e => lookup(2).flatTap(_ => opaque(log, e.getMessage)))
        c <- lookup(b)
      yield a + b + c)
  }

  test("code that fusing would save nothing on is left as written; a def with nothing to save stays a def") {
    // Only opaque calls and recursion: the block and the def as written.
    val opaqueLoop = StromEffect.show {
      def loop(i: Int, acc: Int): IO[Int] = if i == 0 then IO.pure(acc) else lookup(i).flatMap(x => loop(i - 1, acc + x))
      loop(10, 0)
    }
    assert(!opaqueLoop.contains("$run") && !opaqueLoop.contains("while"), opaqueLoop)
    // One synchronous step makes it worth it.
    val withStep = StromEffect.show {
      def loop(i: Int, acc: Int): IO[Int] = if i == 0 then IO.pure(acc) else lookup(i).map(_ + 1).flatMap(x => loop(i - 1, acc + x))
      loop(10, 0)
    }
    assert(withStep.contains("$run"), withStep)
    // A fused block with a def that saves nothing: the block is compiled, the def kept, and the
    // result is the plain one.
    assertEquals(same(log =>
      def fetch(xs: List[Int], acc: Int): IO[Int] = xs match
        case Nil     => IO.pure(acc)
        case x :: tl => lookup(x).flatMap(v => fetch(tl, acc + v))
      for
        a <- IO(log += "start").as(3)
        b <- fetch(List.tabulate(a)(identity), 0)
        _ <- opaque(log, s"b $b")
      yield a + b), 33)
  }
