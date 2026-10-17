package strom.effect

import zio.{Exit, Ref, Runtime, Unsafe, ZIO, durationInt}
import scala.collection.mutable.ListBuffer
import FuzzSupport.Z

/** Runs each generated ZIO program plain and fused: same Exit (value, typed failure or defect)
 *  and the same effect log. Fused in two ways: as `fused` does it, and with everything compiled
 *  (also what `fused` leaves as written) and a yield every 16 steps. A fused program that takes far
 *  longer than the plain one fails too. */
class ZioFuzzSuite extends munit.FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(10, "min")

  def run(p: (ListBuffer[String], Ref[Int]) => Z): (String, List[String], Long) =
    FuzzSupport.reset()
    val log = ListBuffer.empty[String]
    // disconnect: a fused loop that never yields cannot be interrupted; do not wait for it.
    val z = Ref.make(0).flatMap(ref => p(log, ref)).disconnect.timeoutFail("timeout")(10.seconds)
    val t0 = System.nanoTime()
    val exit = Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z.exit).getOrThrowFiberFailure())
    val r = exit match
      case Exit.Success(a) => s"ok $a"
      case Exit.Failure(c) =>
        c.failureOption.map(e => s"fail $e")
          .orElse(c.dieOption.map(t => s"die ${t.getClass.getSimpleName} ${t.getMessage}"))
          .getOrElse(s"other $c")
    (r, FuzzSupport.flat(log), (System.nanoTime() - t0) / 1000000)

  def firstDiff(a: List[String], b: List[String]): String =
    val i = a.zip(b).indexWhere(_ != _) match { case -1 => a.size min b.size; case j => j }
    s"logs differ at $i (sizes ${a.size} / ${b.size}): plain ${a.slice(i, i + 3)} fused ${b.slice(i, i + 3)}"

  test(s"${ZFuzzCases.cases.size} random ZIO programs: fused == plain") {
    var (fails, dies, logged, untouched) = (0, 0, 0L, 0)
    val failures = ZFuzzCases.cases.flatMap { c =>
      val (pr, pl, pt) = run(c.plain)
      if pr.startsWith("fail") then fails += 1
      if pr.startsWith("die") then dies += 1
      logged += pl.size
      if !c.shown(null, null).contains("Fail") then untouched += 1 // fused code has a Fail boundary
      List("fused" -> c.fused, "all compiled, yield every 16" -> c.other).flatMap { (how, prog) =>
        val (fr, fl, ft) = run(prog)
        val problem =
          if fr != pr then Some(s"result: plain $pr, fused $fr")
          else if fl != pl then Some(firstDiff(pl, fl))
          else if ft > 3000 && ft > 20 * pt then Some(s"slow: plain $pt ms, fused $ft ms")
          else None
        problem.map(p => s"case ${c.name} ($how): $p\n  ${c.code.take(1500)}")
      }
    }
    println(s"zio fuzz: ${ZFuzzCases.cases.size} programs, $fails fail, $dies die, $logged log entries, " +
      s"$untouched left unfused (nothing to fuse)")
    assert(failures.isEmpty, failures.take(5).mkString("\n\n") + s"\n\n${failures.size} failing")
  }
