package strom

import scala.util.Random

class MethodSizeSuite extends munit.FunSuite:
  val huge = Strom.segment(Huge.pipeline)

  test("no generated method exceeds HotSpot's 8000-byte JIT limit") {
    val sizes = ClassFiles.all(ClassFiles.testClassesDir).filter(m => m.name == "stepImpl" || m.name.startsWith("part") || m.name == "runImpl")
    assert(sizes.nonEmpty)
    val tooBig = sizes.filter(_.bytes >= 8000)
    assert(tooBig.isEmpty, tooBig.mkString("\n"))
    sizes.sortBy(-_.bytes).take(8).foreach(m => println(s"  ${m.cls}.${m.name}: ${m.bytes} bytes"))
  }

  test("a huge pipeline is split into several methods") {
    val cls = huge.create().getClass.getName.split('.').last
    val parts = ClassFiles.all(ClassFiles.testClassesDir).filter(_.cls == cls + ".class")
    println(parts.map(m => s"${m.name}=${m.bytes}").mkString("  huge: ", ", ", ""))
    assert(parts.count(_.name.startsWith("part")) >= 1, parts.toString)
  }

  test("a huge direct pipeline runs on a frame with split methods and agrees with the reference") {
    val r = Random(8)
    for _ <- 0 until 200 do
      val xs = Array.fill(r.nextInt(30))(r.nextInt(40)); val ys = Array.fill(r.nextInt(30))(r.nextInt(40))
      assertEquals(Strom.run(Huge.direct(xs, ys)), Reference.run(Huge.direct(xs, ys)))
  }

  test("the split huge pipeline agrees with the reference") {
    val r = Random(7)
    for i <- 0 until 150 do
      val xs = Array.fill(r.nextInt(30))(r.nextInt(40)); val ys = Array.fill(r.nextInt(30))(r.nextInt(40))
      val cfg = Harness.randomConfig(r)
      val got = Harness.drive(huge, Seq(xs, ys), cfg)
      assertEquals(got.outputs.toList, Reference.toList(Huge.pipeline(Strom.fromSeq(xs.toSeq), Strom.fromSeq(ys.toSeq))), s"$i $cfg")
  }
