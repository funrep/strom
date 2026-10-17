package iofuse

import zio.{Exit, Runtime, Unsafe, ZIO}
import scala.collection.mutable.ListBuffer

/** [[BuildOrderSuite]] for ZIO. ZIO's operators take their argument by name, so less is built
 *  ahead: receivers, function values, block statements, and the block as a whole. */
class ZioBuildOrderSuite extends munit.FunSuite:
  final class Boom(msg: String) extends RuntimeException(msg)
  type Log = ListBuffer[String]
  type U[A] = ZIO[Any, Nothing, A]

  def outcome[E, A](z: ZIO[Any, E, A]): String =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z.exit).getOrThrowFiberFailure()) match
      case Exit.Success(a) => s"ok $a"
      case Exit.Failure(c) =>
        c.failureOption.map(e => s"fail $e").orElse(c.dieOption.map(t => s"die ${t.getMessage}")).getOrElse(s"other $c")

  inline def same[E, A](inline prog: Log => ZIO[Any, E, A])(using munit.Location): List[String] =
    val (pl, fl) = (ListBuffer.empty[String], ListBuffer.empty[String])
    val p = outcome(prog(pl))
    val f = outcome(IOFuse.fused(prog(fl)))
    assertEquals(f, p)
    assertEquals(fl.toList, pl.toList)
    pl.toList

  inline def sameTwice[E, A](inline prog: Log => ZIO[Any, E, A])(using munit.Location): List[String] =
    val (pl, fl) = (ListBuffer.empty[String], ListBuffer.empty[String])
    val p = { val z = prog(pl); pl += "built"; outcome(z *> z) }
    val f = { val z = IOFuse.fused(prog(fl)); fl += "built"; outcome(z *> z) }
    assertEquals(f, p)
    assertEquals(fl.toList, pl.toList)
    pl.toList

  def built[E, A](log: Log, s: String)(z: ZIO[Any, E, A]): ZIO[Any, E, A] = { log += s"build $s"; z }
  def arg(log: Log, s: String, x: Int): Int = { log += s"arg $s"; x }
  def run(log: Log, s: String): U[Unit] = ZIO.succeed(log += s"run $s").unit
  def failsToBuild(log: Log): ZIO[Any, String, Int] = { log += "building"; throw Boom("build") }
  def fn(log: Log, s: String): Int => U[Int] = { log += s"fn $s"; x => ZIO.succeed(log += s"apply $s").as(x + 1) }

  test("by-name arguments are built when they run") {
    assertEquals(same(log => ZIO.succeed(log += "a") *> built(log, "b")(ZIO.succeed(log += "run b").as(1))), List("a", "build b", "run b"))
    same(log => built(log, "l")(run(log, "l").as(1)) <* built(log, "r")(run(log, "r")))
    same(log => run(log, "a").as(arg(log, "x", 1)).zipRight(ZIO.succeed(arg(log, "y", 2))))
    same(log => run(log, "a") *> ZIO.fail { log += "making"; "e" })
  }

  test("receivers and function values are built first") {
    assertEquals(same(log => ZIO.succeed(log += "a").as(1).flatMap(fn(log, "f"))), List("fn f", "a", "apply f"))
    same(log => built(log, "r")(run(log, "r").as(1)).flatMap(fn(log, "f")).flatMap(fn(log, "g")))
    same(log => run(log, "a").as(1).map { log += "map fn"; (x: Int) => x * 2 })
    same(log => ZIO.succeed(1).flatMap(x => built(log, "r")(run(log, "r").as(x)).flatMap(fn(log, "f"))))
  }

  test("block statements, `if` and `match`") {
    same(log => run(log, "a") *> { log += "stmt"; ZIO.succeed(log += "b") })
    for n <- 0 to 2 do
      same(log => run(log, "a").as(n).flatMap(fn(log, "f")) *> (if arg(log, "c", n) == 1 then built(log, "t")(run(log, "t")) else run(log, "f")))
      same(log => (n match
        case 0 => built(log, "zero")(run(log, "zero").as(0))
        case k if arg(log, "guard", k) == 1 => ZIO.succeed(1)
        case k => run(log, "many").as(k)).flatMap(fn(log, "f")))
  }

  test("an exception thrown while building, under a handler") {
    same(log => ZIO.succeed(1).flatMap(_ => failsToBuild(log).catchAll(_ => ZIO.succeed(-1))))
    same(log => ZIO.succeed(1).flatMap(_ => failsToBuild(log).either.map(_.isLeft)))
    same(log => ZIO.succeed(1).flatMap(_ => (run(log, "a") *> ZIO.fail("run")).catchAll(e => ZIO.succeed(log += e).as(-1))))
  }

  test("built once, run twice") {
    assertEquals(sameTwice(log => built(log, "x")(run(log, "x")).as(1).flatMap(fn(log, "f"))),
      List("build x", "fn f", "built", "run x", "apply f", "run x", "apply f"))
    sameTwice(log => { log += "stmt"; var n = 0; ZIO.succeed { n += 1; log += s"n $n" } *> built(log, "b")(ZIO.succeed(n)) })
    sameTwice(log => if arg(log, "c", 1) == 1 then built(log, "t")(run(log, "t").as(1)) else ZIO.succeed(2))
    sameTwice(log => ZIO.suspendSucceed { var n = 0; ZIO.succeed { n += 1; log += s"n $n" } *> built(log, "b")(ZIO.succeed(n)) })
  }

  test("loops: each iteration builds before it runs") {
    same(log =>
      def loop(i: Int, acc: Int): U[Int] =
        if i == 0 then ZIO.succeed(acc)
        else built(log, s"o$i")(run(log, s"o$i").as(i)).flatMap(fn(log, s"f$i")).flatMap(x => loop(i - 1, acc + x))
      loop(1500, 0))
  }
