package strom

import scala.util.Random

/** Direct (whole-array) pipelines agree with the reference interpreter. */
class DirectSuite extends munit.FunSuite:
  inline def rep(a: Array[Int], b: Array[Int], n: Int) =
    Strom.fromArray(a).flatMap(x => Strom.range(0, x)).zipWith(Strom.fromArray(b).filter(y => y % 2 == 0))(_ * _).take(n).sum
  inline def chain(a: Array[Int]) =
    Strom.fromArray(a).map(_ * 3).filter(_ % 2 == 0).flatMap(x => Strom.range(0, x % 5).map(_ + x)).take(50).count
  inline def zipArrays(a: Array[Int], b: Array[Int]) = Strom.fromArray(a).zipWith(Strom.fromArray(b))(_ * _).sum
  inline def zipFiltered(a: Array[Int], b: Array[Int]) =
    Strom.fromArray(a).zipWith(Strom.fromArray(b).filter(_ > 3))(_ - _).fold(0L)((s, x) => s * 7 + x)
  inline def windows(a: Array[Int]) = Strom.fromArray(a).windowFold(4)(0)(_ max _).sum
  inline def strs(a: Array[String]) = Strom.fromArray(a).filter(_.nonEmpty).map(_.length).sum
  inline def nested3(a: Array[Int]) =
    Strom.fromArray(a).flatMap(x => Strom.range(0, x % 4).flatMap(y => Strom.range(y, x % 6))).take(77).count

  def ints(r: Random) = Array.fill(r.nextInt(40))(r.nextInt(60) - 10)

  test("direct pipelines agree with the reference") {
    val r = Random(42)
    for _ <- 0 until 2000 do
      val a = ints(r); val b = ints(r); val n = r.nextInt(30) - 2
      assertEquals(Strom.run(rep(a, b, n)), Reference.run(rep(a, b, n)))
      assertEquals(Strom.run(chain(a)), Reference.run(chain(a)))
      assertEquals(Strom.run(zipArrays(a, b)), Reference.run(zipArrays(a, b)))
      assertEquals(Strom.run(zipFiltered(a, b)), Reference.run(zipFiltered(a, b)))
      assertEquals(Strom.run(windows(a)), Reference.run(windows(a)))
      assertEquals(Strom.run(nested3(a)), Reference.run(nested3(a)))
      val s = a.map(x => "x" * (x % 3 max 0))
      assertEquals(Strom.run(strs(s)), Reference.run(strs(s)))
  }

  test("ranges, captured values and non-literal functions") {
    val f: Int => Int = _ + 1 // not a literal: called through Function1 (a warning is reported)
    val k = 5
    assertEquals(Strom.run(Strom.range(0, 100).map(f).filter(_ % k == 0).sum), (1 to 100).filter(_ % 5 == 0).sum)
  }
