package iofuse

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*

/** Runs each generated program plain and fused: same result or error, same effect log. Fused in
 *  two ways: as `fused` does it, and with everything compiled (also what `fused` leaves as written),
 *  on frames (the other way of continuing), with a yield every 16 steps. A fused program that takes far longer than the plain one fails too: that is what a
 *  loop that does not yield, or yields late, looks like. */
class FuzzSuite extends munit.FunSuite:
  override val munitTimeout = 10.minutes

  def run(p: (ListBuffer[String], Ref[IO, Int]) => IO[Int]): (Either[String, Int], List[String], Long) =
    FuzzSupport.reset()
    val log = ListBuffer.empty[String]
    val io = Ref[IO].of(0).flatMap(ref => p(log, ref))
    val t0 = System.nanoTime()
    // AndForget: a fused loop that never yields cannot be canceled; do not wait for it.
    val r = io.timeoutAndForget(10.seconds).attempt.unsafeRunSync().left.map(e => s"${e.getClass.getSimpleName}: ${e.getMessage}")
    (r, FuzzSupport.flat(log), (System.nanoTime() - t0) / 1000000)

  def firstDiff(a: List[String], b: List[String]): String =
    val i = a.zip(b).indexWhere(_ != _) match { case -1 => a.size min b.size; case j => j }
    s"logs differ at $i (sizes ${a.size} / ${b.size}): plain ${a.slice(i, i + 3)} fused ${b.slice(i, i + 3)}"

  test(s"${FuzzCases.cases.size} random programs: fused == plain") {
    var (errors, logged, untouched) = (0, 0L, 0)
    val failures = FuzzCases.cases.flatMap { c =>
      val (pr, pl, pt) = run(c.plain)
      if pr.isLeft then errors += 1
      logged += pl.size
      if !c.shown(null, null).contains("IO.defer") then untouched += 1 // fused code is deferred
      List("fused" -> c.fused, "all compiled, frames, yield every 16" -> c.other).flatMap { (how, prog) =>
        val (fr, fl, ft) = run(prog)
        val problem =
          if fr != pr then Some(s"result: plain $pr, fused $fr")
          else if fl != pl then Some(firstDiff(pl, fl))
          else if ft > 3000 && ft > 20 * pt then Some(s"slow: plain $pt ms, fused $ft ms")
          else None
        problem.map(p => s"case ${c.name} ($how): $p\n  ${c.code.take(1500)}")
      }
    }
    println(s"fuzz: ${FuzzCases.cases.size} programs, $errors end in an error, $logged log entries, " +
      s"$untouched left unfused (nothing to fuse)")
    assert(failures.isEmpty, failures.take(5).mkString("\n\n") + s"\n\n${failures.size} failing")
  }
