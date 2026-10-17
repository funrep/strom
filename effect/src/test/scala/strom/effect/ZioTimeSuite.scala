package strom.effect

import zio.{Clock, Duration, Ref, Runtime, Unsafe, ZIO, durationInt}
import zio.test.{TestClock, testEnvironment}
import java.util.concurrent.TimeUnit

/** [[TimeSuite]] for ZIO, under the TestClock (virtual time). */
class ZioTimeSuite extends munit.FunSuite:
  /** Runs z under the test clock, moved forward by `advance`: the value and the milliseconds it took. */
  def exec[A](advance: Duration)(z: ZIO[Any, Nothing, A]): (A, Long) =
    val timed =
      for
        t0 <- Clock.currentTime(TimeUnit.MILLISECONDS)
        a  <- z
        t1 <- Clock.currentTime(TimeUnit.MILLISECONDS)
      yield (a, t1 - t0)
    val prog = (for { f <- timed.fork; _ <- TestClock.adjust(advance); r <- f.join } yield r).provideLayer(testEnvironment)
    Unsafe.unsafe(implicit u =>
      Runtime.default.unsafe.run(prog.timeoutFail(new RuntimeException("test timeout"))(30.seconds)).getOrThrowFiberFailure())

  inline def both[A](advance: Duration)(inline z: ZIO[Any, Nothing, A])(using munit.Location): (A, Long) =
    val p = exec(advance)(z)
    assertEquals(exec(advance)(StromEffect.fused(z)), p)
    p

  test("a loop that sleeps") {
    assertEquals(both(2.seconds) {
      def loop(i: Int, acc: Int): ZIO[Any, Nothing, Int] =
        if i == 100 then ZIO.succeed(acc) else ZIO.sleep(10.millis) *> ZIO.succeed(i).flatMap(x => loop(i + 1, acc + x))
      loop(0, 0)
    }, (4950, 1000L))
  }

  test("timeout: an endless sleeping loop is interrupted at the deadline") {
    assertEquals(both(20.seconds)(Ref.make(0).flatMap { r =>
      def loop(i: Int): ZIO[Any, Nothing, Int] = ZIO.sleep(1.second) *> r.update(_ + 1) *> loop(i + 1)
      loop(0).timeout(10500.millis) *> r.get
    }), (10, 10500L))
  }

  test("race between two loops that sleep") {
    assertEquals(both(1.second) {
      def a(i: Int): ZIO[Any, Nothing, String] = if i == 3 then ZIO.succeed("a") else ZIO.sleep(100.millis) *> a(i + 1)
      def b(i: Int): ZIO[Any, Nothing, String] = if i == 2 then ZIO.succeed("b") else ZIO.sleep(200.millis) *> b(i + 1)
      a(0).race(b(0))
    }, ("a", 300L))
  }

  test("synchronous work takes no time; sleeps in between add up") {
    assertEquals(both(5.seconds) {
      def spin(i: Int, acc: Long): ZIO[Any, Nothing, Long] =
        if i == 5000 then ZIO.succeed(acc) else ZIO.succeed(i * 2L).flatMap(x => spin(i + 1, acc + x))
      for
        a <- spin(0, 0L)
        _ <- ZIO.sleep(1.second)
        b <- spin(0, a)
        _ <- ZIO.sleep(500.millis)
      yield (a, b)
    }, ((24995000L, 49990000L), 1500L))
  }

  test("parallel sleepers: the longer one decides") {
    assertEquals(both(1.second) {
      def nap(i: Int, each: Duration, acc: Int): ZIO[Any, Nothing, Int] =
        if i == 0 then ZIO.succeed(acc) else ZIO.sleep(each) *> ZIO.succeed(acc + i).flatMap(x => nap(i - 1, each, x))
      nap(10, 30.millis, 0).zipPar(nap(4, 100.millis, 0)).map(_ + _)
    }, (65, 400L))
  }
