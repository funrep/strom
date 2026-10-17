package strom.effect

import cats.effect.unsafe.implicits.global
import zio.{Runtime, Unsafe}
import scala.collection.mutable.ListBuffer

/** HotSpot does not JIT-compile a method of 8000 bytes or more. The macro moves large pieces of
 *  generated code into methods of their own; the huge programs (project/EffectFuzzGen.scala) would
 *  not fit otherwise. */
class MethodSizeSuite extends munit.FunSuite:
  lazy val sizes = ClassFiles.all(ClassFiles.testClassesDir)

  test("no method in the compiled tests reaches HotSpot's 8000-byte limit") {
    val tooBig = sizes.filter(_.bytes >= 8000)
    sizes.sortBy(-_.bytes).take(6).foreach(m => println(s"  ${m.cls}.${m.name}: ${m.bytes} bytes"))
    assert(tooBig.isEmpty, tooBig.mkString("\n"))
  }

  test("the huge programs are split into several methods") {
    val parts = sizes.filter(m => m.cls.startsWith("HugeCases") && m.name.startsWith("part$"))
    println(s"  huge programs: ${parts.size} outlined parts, largest ${parts.map(_.bytes).maxOption.getOrElse(0)} bytes")
    assert(parts.size >= 6, parts.size)
  }

  test("the huge programs: fused == plain (cats-effect)") {
    for (name, plain, fused) <- HugeCases.io do
      val (pl, fl) = (ListBuffer.empty[String], ListBuffer.empty[String])
      val p = plain(pl).attempt.unsafeRunSync().left.map(_.toString)
      val f = fused(fl).attempt.unsafeRunSync().left.map(_.toString)
      assertEquals(f, p, name)
      assertEquals(fl.toList, pl.toList, name)
      assert(pl.size >= 60, name)
  }

  test("the huge programs: fused == plain (ZIO)") {
    for (name, plain, fused) <- HugeCases.zio do
      val (pl, fl) = (ListBuffer.empty[String], ListBuffer.empty[String])
      def run(z: FuzzSupport.Z) = Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z.either).getOrThrowFiberFailure())
      val (p, f) = (run(plain(pl)), run(fused(fl)))
      assertEquals(f, p, name)
      assertEquals(fl.toList, pl.toList, name)
      assert(pl.size >= 60, name)
  }
