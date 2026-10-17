package iofuse

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import zio.{Exit, Runtime, Unsafe, ZIO}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*

/** The effects handed to a call the macro does not know are compiled too: an effect argument or
 *  receiver, the body of a lambda that returns an effect, a val that is an effect. */
class OpaqueArgsSuite extends munit.FunSuite:
  type Log = ListBuffer[String]

  def outcome[A](io: IO[A]): Either[String, A] = io.attempt.unsafeRunSync().left.map(_.toString)
  inline def same[A](inline prog: Log => IO[A])(using munit.Location): A =
    val (pl, fl) = (ListBuffer.empty[String], ListBuffer.empty[String])
    val p = outcome(prog(pl))
    val f = outcome(IOFuse.fused(prog(fl)))
    assertEquals(f, p)
    assertEquals(fl.toList, pl.toList)
    p.getOrElse(null.asInstanceOf[A])

  def zout[E, A](z: ZIO[Any, E, A]): String =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z.exit).getOrThrowFiberFailure()) match
      case Exit.Success(a) => s"ok $a"
      case Exit.Failure(c) => s"failed ${c.failureOption}${c.dieOption.map(_.getMessage)}"
  inline def zsame[E, A](inline prog: Log => ZIO[Any, E, A])(using munit.Location): String =
    val (pl, fl) = (ListBuffer.empty[String], ListBuffer.empty[String])
    val p = zout(prog(pl))
    val f = zout(IOFuse.fused(prog(fl)))
    assertEquals(f, p)
    assertEquals(fl.toList, pl.toList)
    p

  def op(log: Log, s: String, v: Int): IO[Int] = IO { log += s"$s $v"; v + 1 }
  def zop(log: Log, s: String, v: Int): ZIO[Any, Nothing, Int] = ZIO.succeed { log += s"$s $v"; v + 1 }
  /** A method the macro cannot see into, given an effect. */
  def twice[A](io: IO[A]): IO[A] = io *> io

  test("cats-effect: masks, finalizers, bracket, parallel branches, a method of the user's") {
    same(log => IO.uncancelable(poll => poll(IO(log += "a").as(1).flatMap(x => IO(x + 1))).flatMap(y => op(log, "o", y).map(_ * 2))))
    same(log => IO(log += "body").as(3).flatMap(x => IO(x * 2)).guarantee(IO(log += "fin").void.flatMap(_ => IO(log += "fin2").void)))
    same(log => IO(log += "acq").as(5).bracket(r => IO(log += s"use $r").as(r).flatMap(x => op(log, "o", x)))(r => IO(log += s"rel $r").void))
    same(log => IO.both(IO(1).flatMap(x => IO(x + 1)).map(_ * 3), IO(10).flatMap(x => IO.pure(x - 1))).flatMap((a, b) => op(log, "sum", a + b)))
    same(log => twice(IO(log += "t").as(2).flatMap(x => IO(log += s"u $x").as(x * 2))))
    same(log => IO(log += "slow").as(1).flatMap(x => op(log, "o", x)).timeout(10.seconds).map(_ + 1))
    same(log => IO(1).flatMap(x => IO.raiseError[Int](new RuntimeException(s"e$x"))).onError(e => IO(log += e.getMessage).void))
  }

  test("cats-effect: the effects inside are fused") {
    val log = ListBuffer.empty[String]
    // The chain inside the lambda and inside the finalizer has no flatMap left.
    val masked = IOFuse.show(IO.uncancelable(_ => IO(log += "a").as(1).flatMap(x => IO(x + 1)).map(_ * 2)))
    assert(masked.contains("uncancelable") && !masked.contains("flatMap"), masked)
    val fin = IOFuse.show(op(log, "o", 1).guarantee(IO(log += "f").void.flatMap(_ => IO(log += "g").void)))
    assert(fin.contains("guarantee") && !fin.contains("flatMap"), fin)
  }

  test("cats-effect: a val that is an effect is built once and fused") {
    assertEquals(same(log => { val part = IO(log += "p").as(2).flatMap(x => IO(x * 5)); part.flatMap(a => part.map(_ + a)) }), 20)
    same(log =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 0 then IO.pure(acc)
        else
          val step = IO(log += s"s$i").as(i).flatMap(x => op(log, "o", x))
          step.guarantee(IO(log += "f").void).flatMap(v => loop(i - 1, acc + v))
      loop(50, 0))
  }

  test("ZIO: masks, finalizers, acquire and release, parallel branches") {
    zsame(log => ZIO.uninterruptibleMask(restore => restore(ZIO.succeed(log += "a").as(1).flatMap(x => ZIO.succeed(x + 1))).flatMap(y => zop(log, "o", y))))
    zsame(log => ZIO.succeed(log += "body").as(3).flatMap(x => ZIO.succeed(x * 2)).ensuring(ZIO.succeed(log += "fin").flatMap(_ => ZIO.succeed(log += "fin2"))))
    zsame(log => ZIO.acquireReleaseWith(ZIO.succeed(log += "acq").as(5))(r => ZIO.succeed(log += s"rel $r"))(r => ZIO.succeed(log += s"use $r").as(r).flatMap(x => zop(log, "o", x))))
    zsame(log => ZIO.succeed(1).flatMap(x => ZIO.succeed(x + 1)).zipPar(ZIO.succeed(10).flatMap(x => ZIO.succeed(x - 1))).flatMap((a, b) => zop(log, "sum", a + b)))
    zsame(log => ZIO.succeed(1).flatMap(x => ZIO.fail(s"e$x")).tapError(e => ZIO.succeed(log += e)))
    val fin = IOFuse.show { val log = ListBuffer.empty[String]; zop(log, "o", 1).ensuring(ZIO.succeed(log += "f").flatMap(_ => ZIO.succeed(log += "g"))) }
    assert(fin.contains("ensuring") && !fin.contains("flatMap"), fin)
  }
