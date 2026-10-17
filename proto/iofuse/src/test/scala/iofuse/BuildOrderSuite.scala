package iofuse

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.collection.mutable.ListBuffer

/** The plain code builds an effect before it runs it. What it evaluates while building (opaque
 *  effect expressions, by-value arguments, block statements) must be evaluated by the fused code at
 *  the same moment: before the effects of the same region run, and once per build. */
class BuildOrderSuite extends munit.FunSuite:
  final class Boom(msg: String) extends RuntimeException(msg)
  type Log = ListBuffer[String]

  def outcome[A](io: IO[A]): Either[String, A] =
    io.attempt.unsafeRunSync().left.map(e => s"${e.getClass.getSimpleName}: ${e.getMessage}")

  inline def same[A](inline prog: Log => IO[A])(using munit.Location): List[String] =
    val (pl, fl) = (ListBuffer.empty[String], ListBuffer.empty[String])
    val p = outcome(prog(pl))
    val f = outcome(IOFuse.fused(prog(fl)))
    assertEquals(f, p)
    assertEquals(fl.toList, pl.toList)
    pl.toList

  /** Builds the effect once and runs it twice. */
  inline def sameTwice[A](inline prog: Log => IO[A])(using munit.Location): List[String] =
    val (pl, fl) = (ListBuffer.empty[String], ListBuffer.empty[String])
    val p = { val io = prog(pl); pl += "built"; outcome(io *> io) }
    val f = { val io = IOFuse.fused(prog(fl)); fl += "built"; outcome(io *> io) }
    assertEquals(f, p)
    assertEquals(fl.toList, pl.toList)
    pl.toList

  /** An effect expression whose evaluation is visible. */
  def built[A](log: Log, s: String)(io: IO[A]): IO[A] = { log += s"build $s"; io }
  /** A by-value argument whose evaluation is visible. */
  def arg(log: Log, s: String, x: Int): Int = { log += s"arg $s"; x }
  def run(log: Log, s: String): IO[Unit] = IO(log += s"run $s").void
  def failsToBuild(log: Log): IO[Int] = { log += "building"; throw Boom("build") }
  def fn(log: Log, s: String): Int => IO[Int] = { log += s"fn $s"; x => IO(log += s"apply $s").as(x + 1) }

  test("`a *> b` builds b before a runs; `a >> b` builds it after") {
    assertEquals(same(log => IO(log += "a") *> built(log, "b")(IO(log += "run b").as(1))), List("build b", "a", "run b"))
    assertEquals(same(log => IO(log += "a") >> built(log, "b")(IO(log += "run b").as(1))), List("a", "build b", "run b"))
    assertEquals(same(log => run(log, "a") *> built(log, "b")(run(log, "b")) *> built(log, "c")(run(log, "c"))),
      List("build b", "build c", "run a", "run b", "run c"))
    same(log => built(log, "l")(run(log, "l").as(1)) <* built(log, "r")(run(log, "r")))
    same(log => IO(log += "a").productR(built(log, "b")(IO.pure(2))).productL(built(log, "c")(IO.unit)))
  }

  test("by-value arguments: pure, as, raiseError") {
    assertEquals(same(log => IO(log += "a").as(arg(log, "x", 1))), List("arg x", "a"))
    assertEquals(same(log => IO(log += "a") *> IO.pure(arg(log, "p", 2))), List("arg p", "a"))
    same(log => run(log, "a") *> IO.raiseError[Int] { log += "making"; Boom("e") })
    same(log => run(log, "a").as(arg(log, "x", 1)).map(_ + 1) <* IO.pure(arg(log, "y", 2)))
  }

  test("function values passed to flatMap and map") {
    assertEquals(same(log => IO(log += "a").as(1).flatMap(fn(log, "f"))), List("fn f", "a", "apply f"))
    same(log => run(log, "a").as(1).flatMap(fn(log, "f")).flatMap(fn(log, "g")))
    same(log => run(log, "a").as(1).map { log += "map fn"; (x: Int) => x * 2 })
  }

  test("a lambda body is built when the lambda runs, in the same order") {
    assertEquals(same(log => IO(log += "a").flatMap(_ => IO(log += "b") *> built(log, "c")(IO.pure(1)))), List("a", "build c", "b"))
    same(log =>
      for
        x <- run(log, "one").as(1)
        y <- run(log, "two") *> built(log, "three")(IO.pure(x + 1)) <* built(log, "four")(run(log, "four"))
        z <- IO(log += "five").as(arg(log, "six", y))
      yield x + y + z)
  }

  test("block statements run when the block is built") {
    assertEquals(same(log => IO(log += "a") *> { log += "stmt"; IO(log += "b") }), List("stmt", "a", "b"))
    same(log => run(log, "a") *> { val n = arg(log, "n", 3); log += "stmt"; run(log, "b").as(n) })
    same(log => run(log, "a") *> { def twice(i: Int): IO[Int] = IO(i * 2); val k = arg(log, "k", 4); twice(k) })
  }

  test("an `if` after an effect: the condition and the chosen branch are built first") {
    for flag <- List(true, false) do
      same(log => IO(log += "a") *> (if flag then built(log, "t")(IO.pure(1)) else built(log, "f")(run(log, "f").as(2))))
      same(log => run(log, "a") *> (if arg(log, "c", 1) == 1 && flag then built(log, "t")(IO.pure(1)) else IO.pure(arg(log, "v", 2))))
      same(log => (if flag then run(log, "t").as(1) else IO.pure(arg(log, "v", 2))) <* built(log, "after")(run(log, "after")))
      same(log => run(log, "a") *> (if flag then { log += "stmt"; IO.pure(1) } else built(log, "f")(IO.pure(2))))
  }

  test("a `match` after an effect") {
    for n <- 0 to 2 do
      same(log => run(log, "a") *> (n match
        case 0 => built(log, "zero")(IO.pure(0))
        case k if arg(log, "guard", k) == 1 => IO(log += "one").as(1)
        case k => built(log, "many")(run(log, "many").as(k))))
  }

  test("an exception thrown while building is not the handler's to catch") {
    assertEquals(same(log => IO(1).flatMap(_ => failsToBuild(log).handleErrorWith(_ => IO.pure(-1)))).headOption, Some("building"))
    same(log => IO(1).flatMap(_ => failsToBuild(log).attempt.map(_.isLeft)))
    same(log => IO(1).flatMap(_ => IO.raiseError[Int] { log += "making"; throw Boom("ctor") }.handleErrorWith(_ => IO.pure(-1))))
    same(log => IO(1).flatMap(_ => (run(log, "a") *> failsToBuild(log)).handleErrorWith(_ => IO.pure(-1))))
    // An error raised when the effect runs is caught as before.
    same(log => IO(1).flatMap(_ => (run(log, "a") *> IO.raiseError[Int](Boom("run"))).handleErrorWith(e => IO(log += e.getMessage).as(-1))))
    for i <- 0 to 1 do
      same(log => IO(i).flatMap(x => (if x == 0 then IO.raiseError[Int] { log += "making"; Boom("zero") } else run(log, "ok").as(x))
        .handleErrorWith(e => IO(log += e.getMessage).as(-1))))
  }

  test("built once, run twice") {
    assertEquals(sameTwice(log => built(log, "x")(run(log, "x")) *> IO.pure(arg(log, "v", 3))),
      List("build x", "arg v", "built", "run x", "run x"))
    sameTwice(log => { log += "stmt"; var n = 0; IO { n += 1; log += s"n $n" } *> built(log, "b")(IO(n)) })
    sameTwice(log => if arg(log, "c", 1) == 1 then built(log, "t")(run(log, "t").as(1)) else IO.pure(2))
    sameTwice(log => (arg(log, "s", 2): @annotation.switch) match
      case 2 => run(log, "two").as(arg(log, "v", 2))
      case _ => IO.pure(0))
    sameTwice(log => IO.defer { var n = 0; IO { n += 1; log += s"n $n" } *> built(log, "b")(IO(n)) })
    sameTwice(log => run(log, "a").as(1).flatMap(fn(log, "f")))
  }

  test("loops: each iteration builds before it runs, also across an opaque step") {
    same(log =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 0 then IO.pure(acc)
        else (IO(log += s"s$i") *> built(log, s"o$i")(IO.pure(i))).flatMap(x => loop(i - 1, acc + x))
      loop(3, 0))
    same(log =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 0 then IO.pure(acc)
        else (run(log, s"s$i") *> built(log, s"o$i")(run(log, s"o$i").as(i)) <* IO.pure(arg(log, s"v$i", i)))
          .flatMap(x => if x % 2 == 0 then loop(i - 1, acc + x) else run(log, "odd") >> loop(i - 1, acc))
      loop(2500, 0))
  }

  // Calling a def builds the start of its body, as the plain code does: before an effect that is
  // written before the call but runs after it, and once for an effect value that runs twice.
  test("a def called by value after an effect: its body is built before the effect runs") {
    assertEquals(same(log =>
      def loop(i: Int): IO[Int] = if i == 0 then IO.pure(0) else built(log, s"o$i")(run(log, s"r$i")) *> loop(i - 1)
      run(log, "first") *> loop(2)), List("build o2", "build o1", "run first", "run r2", "run r1"))
    same(log =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 0 then IO.pure(acc)
        else (run(log, s"s$i") *> built(log, s"o$i")(run(log, s"o$i").as(i)) <* IO.pure(arg(log, s"v$i", i)))
          .flatMap(x => if x % 2 == 0 then loop(i - 1, acc + x) else run(log, "odd") *> loop(i - 1, acc))
      loop(60, 0))
    // Built ahead recursively, like the plain chain: every iteration before anything runs.
    same(log =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 0 then IO.pure(acc) else built(log, s"b$i")(run(log, s"r$i")).as(arg(log, s"a$i", i)) *> loop(i - 1, acc + i)
      loop(40, 0))
    same(log =>
      def even(i: Int): IO[Int] = if i == 0 then IO.pure(0) else built(log, s"e$i")(run(log, s"e$i")) *> odd(i - 1)
      def odd(i: Int): IO[Int] = if i == 0 then IO.pure(1) else IO.pure(arg(log, s"o$i", i)) *> run(log, s"o$i") *> even(i - 1)
      run(log, "start") *> even(9))
  }

  test("an effect made by a def call is built once, however often it runs") {
    assertEquals(sameTwice(log =>
      def loop(i: Int): IO[Int] = if i == 0 then IO.pure(0) else built(log, s"o$i")(run(log, s"r$i")).flatMap(_ => loop(i - 1))
      loop(2)), List("build o2", "built", "run r2", "build o1", "run r1", "run r2", "build o1", "run r1"))
    same(log =>
      def part(k: Int): IO[Int] = { log += s"stmt $k"; val n = arg(log, "n", k); built(log, s"p$k")(run(log, s"p$k").as(n * 2)) }
      val p = part(3)
      run(log, "between") *> p.flatMap(a => p.map(_ + a)))
    same(log =>
      def part(k: Int): IO[Int] =
        if arg(log, "c", k) > 2 then built(log, "big")(run(log, "big").as(k)) else IO.pure(arg(log, "small", k))
      val (p, q) = (part(3), part(1))
      run(log, "between") *> p.flatMap(a => q.flatMap(b => p.map(_ + a + b))))
    // Passed to a function the macro does not see into.
    same(log =>
      def handler(k: Int): IO[Int] = built(log, s"h$k")(run(log, s"h$k").as(k + 1))
      List(1, 2, 3).map(handler).foldLeft(IO.pure(0))((acc, io) => acc.flatMap(a => io.map(_ + a))))
  }

  test("a body that starts with a match, and a match after an effect") {
    same(log =>
      def loop(i: Int, acc: Int): IO[Int] = i match
        case 0 => IO.pure(acc)
        case n if arg(log, s"g$n", n) % 3 == 0 => built(log, s"t$n")(run(log, s"t$n")) *> loop(n - 1, acc + n)
        case n => (run(log, s"s$n") *> built(log, s"o$n")(run(log, s"o$n").as(n))).flatMap(x => run(log, "odd") *> loop(n - 1, acc + x))
      run(log, "first") *> loop(30, 0))
    sameTwice(log =>
      def part(k: Option[Int]): IO[Int] = k match
        case Some(n) => built(log, s"some$n")(run(log, "some").as(n))
        case None => IO.pure(arg(log, "none", 0))
      val (p, q) = (part(Some(2)), part(None))
      run(log, "a") *> p.flatMap(a => q.map(_ + a)))
    for n <- 0 to 2 do
      same(log => run(log, "a") *> (n match
        case 0 => built(log, "zero")(IO.pure(0))
        case k if arg(log, "guard", k) == 1 => IO(log += "one").as(1)
        case k => built(log, "many")(run(log, "many").as(k))).flatMap(v => run(log, s"after $v")))
  }

  test("statements in a branch at the start of a def body, and after an effect") {
    for flag <- List(true, false) do
      same(log =>
        def part(k: Int): IO[Int] =
          if flag then { log += s"stmt $k"; val n = arg(log, "n", k); built(log, s"t$k")(run(log, "t").as(n)) }
          else k match
            case 0 => IO.pure(0)
            case j => { val m = arg(log, "m", j * 2); log += "case"; built(log, s"c$j")(IO.pure(m)) }
        val (p, q) = (part(3), part(4))
        run(log, "a") *> p.flatMap(a => q.map(_ + a)) *> part(5))
      same(log => run(log, "a") *> (if flag then { val v = arg(log, "v", 1); log += "s"; built(log, "b")(IO.pure(v)) } else IO.pure(0)))
    assertEquals(IOFuse.notes {
      def part(k: Int): IO[Int] = if k > 0 then { val n = k * 2; built(ListBuffer.empty, "p")(IO.pure(n)) } else IO.pure(0)
      val p = part(1)
      p *> p
    }, Nil)
  }

  // What is left: a def whose body starts with a local def, a var or a lazy val cannot be built
  // ahead. The compiler says where that shows.
  test("a def that cannot be built ahead gets a compile-time note where it matters") {
    val log = ListBuffer.empty[String]
    val notes = IOFuse.notes {
      def loop(i: Int): IO[Int] =
        def step(j: Int): IO[Int] = built(log, "o")(IO.pure(j))
        if i == 0 then IO.pure(0) else step(i).map(_ + 1).flatMap(_ => run(log, "odd") *> loop(i - 1))
      loop(3)
    }
    assertEquals(notes.count(_.contains("`loop` is called by value")), 1, notes)
    assertEquals(notes.count(_.contains("is used as a value")), 1, notes) // loop(3): built on every run of the block
    // By name: the same order in both. Only the note about the block's own call is left.
    val byName = IOFuse.notes {
      def loop(i: Int): IO[Int] =
        def step(j: Int): IO[Int] = built(log, "o")(IO.pure(j))
        if i == 0 then IO.pure(0) else step(i).map(_ + 1).flatMap(_ => run(log, "odd") >> loop(i - 1))
      loop(3)
    }
    assert(byName.size == 1 && byName.head.contains("is used as a value"), byName)
    // Defs that can be built ahead: nothing to say, however they are called.
    assertEquals(IOFuse.notes {
      def loop(i: Int): IO[Int] = if i == 0 then IO.pure(0) else built(log, "o")(IO.pure(i)).flatMap(_ => run(log, "odd") *> loop(i - 1))
      def other(i: Int): IO[Int] = i match
        case 0 => IO.pure(0)
        case n => built(log, "p")(IO.pure(n)) *> other(n - 1)
      val p = loop(3)
      p *> p *> run(log, "x") *> other(2)
    }, Nil)
    // A body that builds nothing visible (only pure code and delayed steps): nothing to say.
    assertEquals(IOFuse.notes {
      def loop(i: Int, acc: Int): IO[Int] =
        var seen = 0
        if i == 0 then IO.pure(acc) else IO { seen += 1; log += "s" } *> loop(i - 1, acc + i)
      loop(3, 0)
    }, Nil)
  }
