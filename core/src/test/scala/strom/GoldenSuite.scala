package strom

import java.nio.file.{Files, Path, Paths}

/** Golden tests on the generated code (`Expr.show`, captured at compile time).
 *  Run with STROM_UPDATE_GOLDEN=1 to rewrite the expected files after an intended change. */
class GoldenSuite extends munit.FunSuite:
  private val dir = Paths.get("src/test/resources/golden")
  private val update = sys.env.contains("STROM_UPDATE_GOLDEN")

  def golden(name: String, code: String): Unit = test(name) {
    val file = dir.resolve(s"$name.scala.txt")
    if update || !Files.exists(file) then
      Files.createDirectories(dir)
      Files.writeString(file, code + "\n")
    else
      assertNoDiff(code + "\n", Files.readString(file), s"generated code changed; see $file")
  }

  inline def repDirect(a: Array[Int], b: Array[Int], n: Int) =
    Strom.fromArray(a).flatMap(x => Strom.range(0, x)).zipWith(Strom.fromArray(b).filter(y => y % 2 == 0))(_ * _).take(n).sum

  val (a, b, n) = (Array(1), Array(2), 3)
  golden("representative-direct", Strom.showCode(repDirect(a, b, n)))
  golden("representative-segment", Strom.showCode((a: Strom[Int], b: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x)).zipWith(b.filter(y => y % 2 == 0))(_ * _).take(n).sum))
  golden("representative-segment-emit", Strom.showCode((a: Strom[Int], b: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x)).zipWith(b.filter(y => y % 2 == 0))(_ * _).take(n)))
  golden("map-filter-segment", Strom.showCode((a: Strom[Long]) => a.map(_ * 3).filter(_ % 2 == 0)))
  golden("nested-take-direct", Strom.showCode(Strom.fromArray(a).flatMap(x => Strom.range(0, x).map(_ * 2)).take(n).count))
  golden("representative-explain", Strom.explain((a: Strom[Int], b: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x)).zipWith(b.filter(y => y % 2 == 0))(_ * _).take(n).sum))
