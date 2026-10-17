package strom.effect

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import strom.effect.internal.Testing
import scala.concurrent.duration.*

/** Fused code around `sleep` and timeouts, under TestControl (virtual time): the result and the
 *  time that passes are the plain code's. */
class TimeSuite extends munit.FunSuite:
  def timed[A](io: IO[A]): IO[(A, FiniteDuration)] =
    for { t0 <- IO.monotonic; a <- io; t1 <- IO.monotonic } yield (a, t1 - t0)
  def exec[A](io: IO[A]): (A, FiniteDuration) = TestControl.executeEmbed(timed(io)).unsafeRunSync()
  inline def both[A](inline io: IO[A])(using munit.Location): (A, FiniteDuration) =
    val p = exec(io)
    assertEquals(exec(StromEffect.fused(io)), p)
    p

  test("a loop that sleeps") {
    assertEquals(both {
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 100 then IO.pure(acc) else IO.sleep(10.millis) *> IO(i).flatMap(x => loop(i + 1, acc + x))
      loop(0, 0)
    }, (4950, 1.second))
  }

  test("timeout: an endless sleeping loop is canceled at the deadline") {
    assertEquals(both(Ref[IO].of(0).flatMap { r =>
      def loop(i: Int): IO[Int] = IO.sleep(1.second) *> r.update(_ + 1) >> loop(i + 1)
      loop(0).timeoutTo(10500.millis, r.get)
    }), (10, 10500.millis))
  }

  test("timeout that does not fire") {
    assertEquals(both {
      def loop(i: Int): IO[Int] = if i == 5 then IO.pure(i) else IO.sleep(200.millis) >> loop(i + 1)
      loop(0).timeout(2.seconds).map(_ * 2)
    }, (10, 1.second))
  }

  test("race between two loops that sleep") {
    assertEquals(both {
      def a(i: Int): IO[String] = if i == 3 then IO.pure("a") else IO.sleep(100.millis) >> a(i + 1)
      def b(i: Int): IO[String] = if i == 2 then IO.pure("b") else IO.sleep(200.millis) >> b(i + 1)
      IO.race(a(0), b(0)).map(_.merge)
    }, ("a", 300.millis))
  }

  test("synchronous work takes no time; sleeps in between add up") {
    assertEquals(both {
      def spin(i: Int, acc: Long): IO[Long] = if i == 5000 then IO.pure(acc) else IO(i * 2L).flatMap(x => spin(i + 1, acc + x))
      for
        a <- spin(0, 0L)
        _ <- IO.sleep(1.second)
        b <- spin(0, a)
        _ <- IO.sleep(500.millis)
        t <- IO.monotonic
      yield (a, b, t)
    }, ((24995000L, 49990000L, 1500.millis), 1500.millis))
  }

  test("parallel sleepers: the longer one decides") {
    assertEquals(both {
      def nap(i: Int, each: FiniteDuration, acc: Int): IO[Int] =
        if i == 0 then IO.pure(acc) else IO.sleep(each) *> IO(acc + i).flatMap(x => nap(i - 1, each, x))
      IO.both(nap(10, 30.millis, 0), nap(4, 100.millis, 0)).map(_ + _)
    }, (65, 400.millis))
  }

  // A synchronous stretch without a loop cedes too: here 40 steps, with a cede every 8.
  test("a long synchronous stretch cedes every yieldEvery steps") {
    var n = 0
    inline def stretch: IO[Int] =
      for
        a0 <- IO { n += 1; 1 }
        a1 <- IO { n += 1; a0 + 1 }; a2 <- IO { n += 1; a1 + 1 }; a3 <- IO { n += 1; a2 + 1 }
        a4 <- IO { n += 1; a3 + 1 }; a5 <- IO { n += 1; a4 + 1 }; a6 <- IO { n += 1; a5 + 1 }
        a7 <- IO { n += 1; a6 + 1 }; a8 <- IO { n += 1; a7 + 1 }; a9 <- IO { n += 1; a8 + 1 }
        b0 <- IO { n += 1; a9 + 1 }; b1 <- IO { n += 1; b0 + 1 }; b2 <- IO { n += 1; b1 + 1 }
        b3 <- IO { n += 1; b2 + 1 }; b4 <- IO { n += 1; b3 + 1 }; b5 <- IO { n += 1; b4 + 1 }
        b6 <- IO { n += 1; b5 + 1 }; b7 <- IO { n += 1; b6 + 1 }; b8 <- IO { n += 1; b7 + 1 }
        b9 <- IO { n += 1; b8 + 1 }; c0 <- IO { n += 1; b9 + 1 }; c1 <- IO { n += 1; c0 + 1 }
        c2 <- IO { n += 1; c1 + 1 }; c3 <- IO { n += 1; c2 + 1 }; c4 <- IO { n += 1; c3 + 1 }
        c5 <- IO { n += 1; c4 + 1 }; c6 <- IO { n += 1; c5 + 1 }; c7 <- IO { n += 1; c6 + 1 }
        c8 <- IO { n += 1; c7 + 1 }; c9 <- IO { n += 1; c8 + 1 }; d0 <- IO { n += 1; c9 + 1 }
        d1 <- IO { n += 1; d0 + 1 }; d2 <- IO { n += 1; d1 + 1 }; d3 <- IO { n += 1; d2 + 1 }
        d4 <- IO { n += 1; d3 + 1 }; d5 <- IO { n += 1; d4 + 1 }; d6 <- IO { n += 1; d5 + 1 }
        d7 <- IO { n += 1; d6 + 1 }; d8 <- IO { n += 1; d7 + 1 }; d9 <- IO { n += 1; d8 + 1 }
      yield d9
    def cedes(code: String) = "IO\\.cede".r.findAllIn(code).size
    assertEquals(cedes(Testing.showWith(yieldEvery = 8)(stretch)), 4) // five pieces of 8
    assertEquals(cedes(StromEffect.show(stretch)), 0) // 40 steps are under the default interval
    assertEquals(Testing.fusedWith(yieldEvery = 8, gate = true)(stretch).unsafeRunSync(), 40)
    assertEquals(n, 40)
    // Left-nested, and maps only.
    inline def maps: IO[Int] = IO(1).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1)
      .flatMap(x => IO(x * 2)).flatMap(x => IO(x + 1)).flatMap(x => IO(x + 1)).flatMap(x => IO(x + 1)).flatMap(x => IO(x + 1))
    assert(cedes(Testing.showWith(yieldEvery = 4)(maps)) >= 2)
    assertEquals(Testing.fusedWith(yieldEvery = 4, gate = true)(maps).unsafeRunSync(), maps.unsafeRunSync())
  }
