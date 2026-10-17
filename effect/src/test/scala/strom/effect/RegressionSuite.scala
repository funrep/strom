package strom.effect

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import scala.collection.mutable.ListBuffer
import FuzzSupport.*

/** Regressions found by the fuzzer. */
class RegressionSuite extends munit.FunSuite:
  inline def same(inline p: (ListBuffer[String], Ref[IO, Int]) => IO[Int])(using munit.Location): Unit =
    def go(f: (ListBuffer[String], Ref[IO, Int]) => IO[Int]) =
      val log = ListBuffer.empty[String]
      (Ref[IO].of(0).flatMap(r => f(log, r)).attempt.unsafeRunSync().left.map { e => e.printStackTrace(); e.toString }, log.toList)
    assertEquals(go((l, r) => StromEffect.fused(p(l, r))), go(p))

  val x4 = 5; val acc3 = 7; val a13 = 3; val i2 = 2
  // Found by the fuzzer: a join point in a loop, reached both directly and after an opaque call.
  test("join point in a loop, called after an opaque step") {
    same((log, ref) =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i <= 0 then IO.pure(acc)
        else
          def unused(j: Int): IO[Int] = IO.pure(j)
          (if i % 2 == 0 then IO(step(log, "s", i)) else opq(log, "o", i)).flatMap(x => loop(i - 1, acc + x))
      loop(1500, 0))
  }

  // `_ + _` on a pair expands to code whose types name the lambda's parameter (`x.type`).
  test("a lambda over a tuple, written with placeholders") {
    same((log, ref) => IO.both(IO(1), IO(2)).map(_ + _).flatMap(x => IO.pure((x, step(log, "x", x + 1))).map(_ * _)))
    same((log, ref) => IO.pure((3, 4)).flatMap((a, b) => opq(log, "o", a).map(_ + b)))
    same((log, ref) =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 0 then IO.pure(acc) else IO.pure((i, acc)).map(_ + _).flatMap(v => loop(i - 1, v))
      loop(2000, 0))
  }

  // A closure made in one iteration and run after the loop has moved on must see that iteration's
  // values. (With the loop's state in shared variables it saw the last iteration's.)
  test("a closure that outlives its iteration keeps the iteration's values") {
    import cats.syntax.all.*
    same((log, ref) =>
      def loop(i: Int, later: List[IO[Int]]): IO[List[IO[Int]]] =
        if i == 4 then IO.pure(later)
        else opq(log, "o", i).flatMap(x => loop(i + 1, IO(i * 100 + x) :: later))
      loop(0, Nil).flatMap(_.sequence).map(_.zipWithIndex.map(_ * _).sum))
    same((log, ref) =>
      def loop(i: Int, acc: Int, fibers: List[cats.effect.FiberIO[Int]]): IO[Int] =
        if i == 20 then fibers.traverse(_.joinWithNever).map(_.sum + acc)
        else
          for
            x <- opq(log, "o", i)
            f <- (IO.cede *> IO.cede *> IO(i * 1000 + x)).start
            y <- opq(log, "p", x)
            r <- loop(i + 1, acc + y, f :: fibers)
          yield r
      loop(0, 0, Nil))
  }
