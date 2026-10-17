import sbt._
import scala.util.Random

/** Generates random cats-effect IO and ZIO programs for strom-effect's fuzz suites. The macro must see the
 *  code, so each program is written out as source, once plain and once inside StromEffect.fused.
 *
 *  Programs mix synchronous steps that log (`IO(step(..))`), opaque calls (`opq`, `ref`, `cede`),
 *  binds, branches, matches with binds, errors, handlers, local vals and vars, and local loops
 *  (tail, non-tail, mutual, used as values) that run past the 1024-iteration cede.
 *
 *  Building an effect is visible too: effect expressions, by-value arguments, function values and
 *  block statements that log when they are evaluated (`bld`, `IO.pure(step(..))`, `fnv`). The fused
 *  code must evaluate them when the plain code does, also at the start of a local def's body,
 *  which is built when the def is called. The macro can do that unless the body starts with a
 *  local def, var or lazy val; `hd` keeps what is generated at the start of a def body to what
 *  stays exact (see `io`).
 *
 *  Programs also fork: two branches run in parallel, as a forked fiber that is joined, or one
 *  branch races an endless fused loop that must be canceled. A branch is a local def, so its body
 *  is fused; it has its own log (`sub`) and its own Ref, and its outcome is caught, so the result
 *  and the logs do not depend on how the fibers interleave.
 *
 *  STROM_FUZZ_CASES (default 150) and STROM_FUZZ_SEED (default 1) control the output. */
object EffectFuzzGen {
  private def cases: Int = sys.env.get("STROM_FUZZ_CASES").map(_.toInt).getOrElse(150)
  private def seed: Long = sys.env.get("STROM_FUZZ_SEED").map(_.toLong).getOrElse(1L)

  /** How a program is spelled for one effect type. */
  private abstract class Syntax {
    def T: String                       // the program type
    def pure(v: String): String
    def delay(body: String): String
    def opq(t: String, v: String): String
    def cede(v: String): String
    def raise(t: String): String        // a failure the handlers catch
    def boom(t: String, v: String): String
    def unitAs(v: String): String
    def defer(x: String): String
    def seqOps: Seq[String]
    def byName(op: String): Boolean     // is the operator's argument built when it runs?
    def bld(t: String, x: String): String
    def bldx(t: String, v: String): String // an effect expression that throws for some values while it is built
    def fnv(t: String): String
    def refMake(v: String): String
    /** `a` under a cancelation mask with `b` after it, and `a` with a finalizer that logs. The macro
     *  compiles the effects handed to these calls (the lambda's body, the finalizer). */
    def mask(a: String, b: String): String
    def fin(a: String, t: String): String
    /** Two branches (effects of Int) run concurrently. kind 0: in parallel, 1: one forked and
     *  joined, 2: the first races the endless loop `spin`. Their outcomes are caught. */
    def par(kind: Int, a: String, b: String, spin: String): String
    def widen(x: String): String        // so handlers apply (ZIO: E must not be Nothing)
    def handle: String
    def logError(e: String): String     // log the caught error, as an expression on the log
    def attempt: String
  }
  private object IOSyntax extends Syntax {
    val T = "IO[Int]"
    def pure(v: String) = s"IO.pure($v)"
    def delay(body: String) = s"IO($body)"
    def opq(t: String, v: String) = s"""opq(log, "$t", $v)"""
    def cede(v: String) = s"IO.cede.as($v)"
    def raise(t: String) = s"""IO.raiseError[Int](Boom("$t"))"""
    def boom(t: String, v: String) = s"""IO(boom(log, "$t", $v))"""
    def unitAs(v: String) = s"IO.unit.as($v)"
    def defer(x: String) = s"IO.defer($x)"
    val seqOps = Seq("*>", ">>", "<*")
    def byName(op: String) = op == ">>"
    def bld(t: String, x: String) = s"""bld(log, "$t", $x)"""
    def bldx(t: String, v: String) = s"""bldx(log, "$t", $v)"""
    def fnv(t: String) = s"""fnv(log, "$t")"""
    def refMake(v: String) = s"Ref[IO].of($v)"
    def mask(a: String, b: String) = s"IO.uncancelable(poll => poll($a).flatMap(m => $b.map(_ + m)))"
    def fin(a: String, t: String) = s"""$a.guarantee(IO(step(log, "$t", 0)).void)"""
    def par(kind: Int, a: String, b: String, spin: String) = kind match {
      case 0 => s"IO.both($a.attempt, $b.attempt).map { case (x, y) => x.getOrElse(-1) + y.getOrElse(-2) }"
      case 1 => s"$a.attempt.start.flatMap(f => $b.attempt.flatMap(y => f.joinWithNever.map(x => x.getOrElse(-1) + y.getOrElse(-2))))"
      case _ => s"IO.race($a.attempt, $spin).map { case Left(x) => x.getOrElse(-1); case Right(_) => -9 }"
    }
    def widen(x: String) = x
    val handle = "handleErrorWith"
    def logError(e: String) = s"log += $e.getMessage"
    val attempt = "attempt"
  }
  private object ZIOSyntax extends Syntax {
    val T = "Z"
    def pure(v: String) = s"ZIO.succeed($v)"
    def delay(body: String) = s"ZIO.succeed($body)"
    def opq(t: String, v: String) = s"""opqZ(log, "$t", $v)"""
    def cede(v: String) = s"ZIO.yieldNow.as($v)"
    def raise(t: String) = s"""(ZIO.fail("$t"): Z)"""                                      // not Nothing: binds use the value
    def boom(t: String, v: String) = java.lang.Math.floorMod(t.hashCode, 3) match {
      case 0 => s"""ZIO.succeed(boom(log, "$t", $v))"""                                      // a defect
      case 1 => s"""ZIO.attempt(boom(log, "$t", $v)).mapError(_.getMessage)"""             // a failure (opaque)
      case _ => s"""ZIO.attempt(boom(log, "$t", $v)).catchAll(e => ZIO.fail(e.getMessage))""" // a failure (fused)
    }
    def unitAs(v: String) = s"ZIO.unit.as($v)"
    def defer(x: String) = s"ZIO.suspendSucceed($x)"
    val seqOps = Seq("*>", "<*", "zipRight")
    def byName(op: String) = true
    def bld(t: String, x: String) = s"""bldZ(log, "$t", $x)"""
    def bldx(t: String, v: String) = s"""bldxZ(log, "$t", $v)"""
    def fnv(t: String) = s"""fnvZ(log, "$t")"""
    def refMake(v: String) = s"Ref.make($v)"
    def mask(a: String, b: String) = s"ZIO.uninterruptibleMask(restore => restore($a).flatMap(m => $b.map(_ + m)))"
    def fin(a: String, t: String) = s"""$a.ensuring(ZIO.succeed(step(log, "$t", 0)))"""
    def par(kind: Int, a: String, b: String, spin: String) = kind match {
      case 0 => s"$a.exit.zipPar($b.exit).map { case (x, y) => x.getOrElse(_ => -1) + y.getOrElse(_ => -2) }"
      case 1 => s"$a.exit.fork.flatMap(f => $b.exit.flatMap(y => f.join.map(x => x.getOrElse(_ => -1) + y.getOrElse(_ => -2))))"
      case _ => s"$a.exit.raceEither($spin).map { case Left(x) => x.getOrElse(_ => -1); case Right(_) => -9 }"
    }
    def widen(x: String) = s"($x: Z)"
    val handle = "catchAll"
    def logError(e: String) = s"log += $e"
    val attempt = "either"
  }

  private final class Gen(r: Random, id: Int, S: Syntax) {
    private var n = 0
    private var loops = 0 // nesting depth: inner loops stay short
    private var masked = 0 // inside a cancelation mask: no forks (a ZIO child inherits the mask, so a raced endless loop could not be stopped)
    private def fresh(p: String): String = { n += 1; s"$p$n" }
    private def tag(): String = { n += 1; s"$id.$n" }
    private def pick[A](xs: Seq[A]): A = xs(r.nextInt(xs.size))
    private def chance(k: Int): Boolean = r.nextInt(k) == 0

    /** An Int expression over the immutable names in scope. */
    def value(scope: List[String]): String = r.nextInt(6) match {
      case 0 | 1 if scope.nonEmpty => pick(scope)
      case 2 if scope.nonEmpty => s"(${pick(scope)} ${pick(Seq("+", "-", "*", "^"))} ${value(scope)})"
      case 3 if scope.nonEmpty => s"(${pick(scope)} % ${2 + r.nextInt(5)})"
      case _ => (r.nextInt(40) - 8).toString
    }

    /** An IO[Int] expression. vars: mutable Ints, only touched inside IO { }. hd: where we are.
     *  0: anywhere. 1: at the start of a def body that the macro builds late (it may have local
     *  defs): nothing visible is built there. 2, 3: at the start of a def body it builds ahead
     *  (3: in a branch): building is visible, but there are no local defs. Lambda bodies and
     *  by-name arguments are built later: 0 again. */
    def io(scope: List[String], vars: List[String], d: Int, hd: Int = 0): String =
      if (d <= 0 || chance(7)) leaf(scope, vars, hd)
      else r.nextInt(26) match {
        case 0 | 1 =>
          val x = fresh("x")
          s"${io(scope, vars, d - 1, hd)}.flatMap($x => ${io(x :: scope, vars, d - 1)})"
        case 2 =>
          val x = fresh("x")
          s"${io(scope, vars, d - 1, hd)}.map($x => ${value(x :: scope)})"
        case 3 =>
          val op = pick(S.seqOps)
          s"(${io(scope, vars, d - 1, hd)} $op ${io(scope, vars, d - 1, if (S.byName(op)) 0 else hd)})"
        case 4 => s"${io(scope, vars, d - 1, hd)}.as(${if (hd == 1 || chance(2)) value(scope) else builtValue(scope)})"
        case 5 =>
          val (a, b, c) = (fresh("a"), fresh("b"), fresh("c"))
          s"(for { $a <- ${io(scope, vars, d - 1, hd)}; $b <- ${io(a :: scope, vars, d - 1)}; $c = ${value(a :: b :: scope)}; " +
            s"r <- ${io(c :: b :: scope, vars, d - 1)} } yield r + $a)"
        case 6 => s"(if (${value(scope)} % 2 == 0) ${io(scope, vars, d - 1, branch(hd))} else ${io(scope, vars, d - 1, branch(hd))})"
        case 7 =>
          val (x, m) = (fresh("x"), fresh("m"))
          s"${io(scope, vars, d - 1, hd)}.flatMap { case $x if $x % 3 == 0 => ${io(x :: scope, vars, d - 1)}; " +
            s"case $m => ${io(m :: scope, vars, d - 1)} }"
        case 8 =>
          val (x, a, b) = (fresh("x"), fresh("a"), fresh("b"))
          s"${io(scope, vars, d - 1, hd)}.map($x => ($x, $x * 2)).flatMap { case ($a, $b) => ${io(a :: b :: scope, vars, d - 1)} }"
        case 9 =>
          val e = fresh("e")
          val handler = if (chance(2)) s"${S.delay(S.logError(e))}.as(${value(scope)})" else io(scope, vars, d - 1)
          s"${S.widen(risky(scope, vars, d - 1, hd))}.${S.handle}($e => $handler)"
        case 10 =>
          val v = fresh("v")
          s"${S.widen(risky(scope, vars, d - 1, hd))}.${S.attempt}.flatMap { case Right($v) => ${io(v :: scope, vars, d - 1)}; case Left(_) => ${S.pure("-1")} }"
        case 11 =>
          val y = fresh("y")
          s"{ val $y = ${value(scope)}; ${io(y :: scope, vars, d - 1, hd)} }"
        case 12 =>
          val v = fresh("cnt")
          // defer: a var made when the effect is built would be shared by its reruns.
          S.defer("").stripSuffix("()") + s" { var $v = ${value(scope)}; ${io(scope, v :: vars, d - 1)}.flatMap(z => ${io("z" :: scope, v :: vars, d - 1)}) }"
        case 13 | 14 if hd <= 1 => loop(scope, vars, d, quiet = hd == 1)
        case 15 => S.defer(io(scope, vars, d - 1))
        case 16 =>
          val f = fresh("f")
          s"{ val $f: Int => ${S.T} = q => ${leaf("q" :: scope, vars, 0)}; ${io(scope, vars, d - 1, hd)}.flatMap($f) }"
        // Visible building: a function value, a block statement, an effect expression.
        case 17 if hd != 1 => s"${io(scope, vars, d - 1, hd)}.flatMap(${S.fnv(tag())})"
        case 18 if hd != 1 => s"""{ log += "s${tag()}"; val ${fresh("y")} = ${builtValue(scope)}; ${io(scope, vars, d - 1, hd)} }"""
        case 19 if hd != 1 => S.bld(tag(), io(scope, vars, d - 1))
        case 20 | 21 if d >= 2 && masked == 0 && hd <= 1 => par(scope, d)
        case 22 =>
          masked += 1
          try S.mask(io(scope, vars, d - 1), io(scope, vars, d - 1)) finally masked -= 1
        case 23 => S.fin(io(scope, vars, d - 1, hd), tag())
        // A step that logs, then an effect whose building logs: the operator decides which is first.
        case 24 | 25 if hd != 1 =>
          s"""(${S.delay(s"""step(log, "${tag()}", ${value(scope)})""")} ${pick(S.seqOps)} ${S.bld(tag(), leaf(scope, vars, 0))})"""
        case _ => leaf(scope, vars, hd)
      }

    private def branch(hd: Int): Int = if (hd == 2) 3 else hd

    /** Concurrent branches. Each is a def (so the macro compiles it) with its own Ref and log, and
     *  no access to the enclosing vars. */
    def par(scope: List[String], d: Int): String = {
      val (pl, a, b, u, spin, j) = (fresh("pl"), fresh("par"), fresh("par"), fresh("u"), fresh("spin"), fresh("j"))
      def branch(name: String) =
        s"""def $name($u: Int): ${S.T} = ${S.refMake(u)}.flatMap { ref => val log = sub($pl, "${tag()}"); ${io(u :: scope, Nil, d - 1)} }"""
      val spinDef = s"def $spin($j: Int): ${S.T} = ${S.delay(s"$j + 1")}.flatMap(k => $spin(k))"
      s"{ val $pl = log; ${branch(a)}; ${branch(b)}; $spinDef; ${S.par(r.nextInt(3), s"$a(${value(scope)})", s"$b(${value(scope)})", s"$spin(0)")} }"
    }

    /** An Int whose evaluation logs: visible when it is a by-value argument. */
    def builtValue(scope: List[String]): String = s"""step(log, "${tag()}", ${value(scope)})"""

    /** The body of a handled region: often ends in an error, at once or after some steps (errors
     *  are rare leaves, so plain `io` seldom puts one inside a handler). */
    def risky(scope: List[String], vars: List[String], d: Int, hd: Int = 0): String = {
      def err() = r.nextInt(if (hd == 1) 2 else 3) match {
        case 0 => S.raise(tag())
        case 1 => S.boom(tag(), value(scope))
        case _ => S.bldx(tag(), value(scope)) // may throw while it is built: not the handler's to catch
      }
      r.nextInt(6) match {
        case 0 => err()
        case 1 => s"(${leaf(scope, vars, hd)} ${S.seqOps.head} ${err()})"
        case 2 => s"(if (${value(scope)} % 2 == 0) ${io(scope, vars, d, branch(hd))} else ${err()})"
        case 3 => val x = fresh("x"); s"${io(scope, vars, d, hd)}.flatMap($x => if ($x % 2 == 0) ${S.pure(x)} else ${err()})"
        case _ => io(scope, vars, d, hd)
      }
    }

    def leaf(scope: List[String], vars: List[String], hd: Int): String = r.nextInt(14) match {
      case 12 if hd != 1 => S.pure(builtValue(scope)) // by value on IO: evaluated when built
      case 13 if hd != 1 => S.bld(tag(), S.opq(tag(), value(scope)))
      case 0 | 1 => S.pure(value(scope))
      case 2 | 3 => S.delay(s"""step(log, "${tag()}", ${value(scope)})""")
      case 4 | 5 => S.opq(tag(), value(scope))
      case 6 => s"ref.updateAndGet(_ + ${value(scope)})"
      case 7 => S.cede(value(scope))
      case 8 if vars.nonEmpty => val v = pick(vars); S.delay(s"""{ $v += ${value(scope)}; step(log, "${tag()}", $v) }""")
      case 9 if chance(3) => S.raise(tag())
      case 10 if chance(2) => S.boom(tag(), value(scope))
      case _ => S.unitAs(value(scope))
    }

    /** quiet: the loop is defined at the start of a def body that is built late, so what it builds
     *  at its own start must not be visible either. Otherwise half of the loops build visibly
     *  at their start (in a branch of the def's `if`: hd 3). */
    def loop(scope: List[String], vars: List[String], d: Int, quiet: Boolean = false): String = {
      val h = if (quiet || chance(2)) 1 else 3
      val (l, i, acc, x) = (fresh("loop"), fresh("i"), fresh("acc"), fresh("x"))
      val inner = i :: acc :: scope
      val iters = if (loops > 0) pick(Seq(0, 1, 3, 10)) else pick(Seq(0, 1, 3, 40, 1500))
      loops += 1
      def rec(a: String) = s"$l($i - 1, $a)"
      val body = r.nextInt(9) match {
        // The recursive call after an effect, by value or by name: by value, the plain code builds
        // every iteration before the first one runs.
        case 7 | 8 => s"(${io(inner, vars, d - 1, h)} ${pick(S.seqOps.filterNot(_ == "<*"))} ${rec(s"$acc + $i")})"
        case 0 => s"${io(inner, vars, d - 1, h)}.flatMap($x => ${rec(s"$acc + $x")})"
        case 1 => s"(for { $x <- ${io(inner, vars, d - 1, h)}; y <- ${io(x :: inner, vars, d - 1)}; r <- ${rec(s"$acc ^ y")} } yield r)"
        case 2 => s"(if ($i % 3 == 0) ${io(inner, vars, d - 1, h)}.flatMap($x => ${rec(s"$acc + $x")}) else ${rec(s"$acc + 1")})"
        case 3 => s"${S.pure(s"$i % 4")}.flatMap { case 0 => ${io(inner, vars, d - 1)}.flatMap($x => ${rec(s"$acc + $x")}); case k => ${rec(s"$acc + k")} }"
        case 4 => s"${S.defer(rec(acc))}.map(r => r + ${value(inner)})"
        case 5 => s"${S.widen(risky(inner, vars, d - 1, h))}.${S.handle}(_ => ${S.pure("-1")}).flatMap($x => ${rec(s"$acc + $x")})"
        case _ => s"${io(inner, vars, d - 1, h)}.flatMap($x => if ($x % 5 == 0) ${io(x :: inner, vars, d - 1)} else ${rec(s"$acc + $x")})"
      }
      loops -= 1
      val defn = s"def $l($i: Int, $acc: Int): ${S.T} = if ($i <= 0) ${io(acc :: scope, vars, d - 1, h)} else $body"
      r.nextInt(7) match {
        case 5 | 6 => // called after an effect: by value, the start of the body is built before that effect runs
          s"""{ $defn; (${S.delay(s"""step(log, "${tag()}", ${value(scope)})""")} ${pick(S.seqOps)} $l(${pick(Seq(1, 3, 10))}, ${value(scope)})) }"""
        case 0 => // used as a value, run twice
          val p = fresh("p")
          s"{ $defn; val $p = $l($iters, ${value(scope)}); $p.flatMap(u => $p.map(_ + u)) }"
        case 1 => // mutual recursion
          val (m, j) = (fresh("other"), fresh("j"))
          s"{ $defn; def $m($j: Int): ${S.T} = if ($j <= 0) ${S.pure("0")} else ${io(j :: scope, vars, 1, h)}.flatMap(_ => $l($j - 1, $j)); " +
            s"$m(${pick(Seq(2, 30))}) }"
        case _ => s"{ $defn; $l($iters, ${value(scope)}) }"
      }
    }
  }

  def apply(dir: File): Seq[File] =
    write(dir, IOSyntax, "FuzzCases", "FuzzCase", "cats.effect.{IO, Ref}", "Ref[IO, Int]", "IO[Int]") ++
      write(dir, ZIOSyntax, "ZFuzzCases", "ZFuzzCase", "zio.{Ref, ZIO}", "Ref[Int]", "Z") :+ huge(dir)

  /** Programs whose fused code is far over HotSpot's 8000-byte method limit unless it is split:
   *  a long synchronous chain, a loop body of many states, a wide match. Fixed, not random. */
  private def hugePrograms(S: Syntax): Seq[(String, String)] = {
    def mix(x: String, i: Int) = s"(($x * 31 + $i) ^ ($x >>> 3)) + ($x & ${i + 5}) - ($x % 7)"
    // Few steps (the Scala compiler's own stack limits the nesting), each a lot of code.
    def heavy(t: String, x: String, i: Int) =
      s"""{ val p = ${mix(x, i)}; val q = ${mix("p", i + 1)}; val u = ${mix("q", i + 2)}; val w = ${mix("u", i + 3)}; step(log, "$t", ${mix("w", i + 4)}) }"""
    val chain = {
      val steps = (1 to 60).map(i => s"""x$i <- ${S.delay(heavy(s"c$i", s"x${i - 1}", i))}""")
      s"""(for { x0 <- ${S.pure("1")}; ${steps.mkString("; ")} } yield x60)"""
    }
    val loop = {
      val steps = (1 to 60).map { i =>
        if (i % 2 == 0) s"""a$i <- ${S.opq(s"h$i", s"a${i - 1} & 0xff")}"""
        else s"""a$i <- ${S.delay(heavy(s"h$i", s"a${i - 1}", i))}"""
      }
      s"""{ def loop(i: Int, acc: Int): ${S.T} = if (i == 0) ${S.pure("acc")} else (for { a0 <- ${S.pure("i")}; ${steps.mkString("; ")}; """ +
        s"""r <- loop(i - 1, acc ^ a60) } yield r); loop(4, 0) }"""
    }
    val wide = {
      val cases = (0 until 70).map { n =>
        val steps = (1 to 6).map(j => s"""y$j <- ${S.delay(s"""step(log, "m${n}_$j", ${mix(s"y${j - 1}", n + j)})""")}""")
        s"""case $n => (for { y0 <- ${S.pure(n.toString)}; ${steps.mkString("; ")} } yield y6)"""
      }
      s"""{ def loop(i: Int, acc: Int): ${S.T} = if (i == 140) ${S.pure("acc")} else ${S.delay("i % 70")}.flatMap { ${cases.mkString("; ")} }""" +
        s""".flatMap(v => loop(i + 1, acc ^ v)); loop(0, 0) }"""
    }
    Seq("chain" -> chain, "loop" -> loop, "wide" -> wide)
  }

  private def huge(dir: File): File = {
    def group(S: Syntax, name: String, t: String) = {
      val ps = hugePrograms(S).map { case (n, code) =>
        s"""    ("$n", (log: ListBuffer[String]) => $code, (log: ListBuffer[String]) => StromEffect.fused { $code }),"""
      }
      s"""  val $name: List[(String, ListBuffer[String] => $t, ListBuffer[String] => $t)] = List(\n${ps.mkString("\n")}\n  )"""
    }
    val f = dir / "strom" / "effect" / "HugeCases.scala"
    IO.write(f,
      s"""package strom.effect
         |
         |// Generated by project/EffectFuzzGen.scala. Do not edit.
         |import cats.effect.IO
         |import zio.ZIO
         |import scala.collection.mutable.ListBuffer
         |import FuzzSupport.*
         |
         |object HugeCases:
         |${group(IOSyntax, "io", "IO[Int]")}
         |${group(ZIOSyntax, "zio", "Z")}
         |""".stripMargin)
    f
  }

  /** Each program is also fused with a yield every 16 steps and with everything compiled (`fused`
   *  leaves code that would save nothing as written), so the cedes and the whole compiler are
   *  tested on every program. */
  private def write(dir: File, S: Syntax, obj: String, cls: String, imports: String, ref: String, t: String): Seq[File] = {
    val r = new Random(seed)
    val body = (0 until cases).map { i =>
      val p = new Gen(r, i, S).io(Nil, Nil, 3 + r.nextInt(4))
      val code = p.replace("\\", "\\\\").replace("\"", "\\\"")
      s"""    $cls("$i", "$code", (log, ref) => $p, (log, ref) => StromEffect.fused { $p }, (log, ref) => StromEffect.show { $p }, """ +
        s"""(log, ref) => strom.effect.internal.Testing.fusedWith(yieldEvery = 16, gate = false) { $p }),"""
    }
    // 30 programs per object (one object would exceed the JVM's class size limit).
    val chunks = body.grouped(30).toList
    val header =
      s"""package strom.effect
         |
         |// Generated by project/EffectFuzzGen.scala (seed $seed). Do not edit.
         |import $imports
         |import scala.collection.mutable.ListBuffer
         |import FuzzSupport.*
         |""".stripMargin
    IO.delete((dir / "strom" / "effect" * s"$obj*.scala").get) // stale chunks from a larger run
    val parts = chunks.zipWithIndex.map { case (cs, i) =>
      val f = dir / "strom" / "effect" / s"$obj$i.scala"
      IO.write(f, header + s"""
         |object $obj$i:
         |  val cases: Vector[$cls] = Vector(
         |${cs.mkString("\n")}
         |  )
         |""".stripMargin)
      f
    }
    val file = dir / "strom" / "effect" / s"$obj.scala"
    IO.write(file, header + s"""
         |final case class $cls(name: String, code: String,
         |  plain: (ListBuffer[String], $ref) => $t, fused: (ListBuffer[String], $ref) => $t,
         |  shown: (ListBuffer[String], $ref) => String, other: (ListBuffer[String], $ref) => $t)
         |
         |object $obj:
         |  val cases: Vector[$cls] = ${chunks.indices.map(i => s"$obj$i.cases").mkString(" ++ ")}
         |""".stripMargin)
    file +: parts
  }
}
