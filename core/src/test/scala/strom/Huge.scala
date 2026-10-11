package strom

/** Deliberately huge pipelines, built from inline helpers: they force method splitting. */
object Huge:
  inline def mix(inline v: Int): Int =
    ((v * 31 + 7) ^ (v >>> 3)) * 17 + ((v << 2) | 5) - (v % 11) * 13 + (v & 255) * (v | 3) - ((v + 1) * (v + 2)) % 97

  inline def m2(inline s: Strom[Int]): Strom[Int] =
    s.map(y => { val v = y; mix(v) }).map(y => { val v = y; mix(v) })
  inline def m6(inline s: Strom[Int]): Strom[Int] = m2(m2(m2(s)))
  inline def m12(inline s: Strom[Int]): Strom[Int] = m6(m6(s))

  /** `n` ranges per element, each through 12 maps and a filter, zipped and mapped 6 more times. */
  inline def expand(inline s: Strom[Int], inline n: Int, inline f: (Int, Int) => Int): Strom[Int] =
    s.flatMap(x => m6(m12(Strom.range(0, x % n)).filter(_ % 3 != 1)
      .zipWith(m12(Strom.range(0, x % (n + 1))).filter(_ % 3 != 1))(f)))

  inline def body(inline a: Strom[Int], inline b: Strom[Int]): Strom[Int] =
    m2(m2(m6(expand(a, 3, _ + _).zipWith(expand(b, 5, _ - _))(_ ^ _)))).filter(_ % 5 != 0).windowFold(3)(0)(_ + _)

  inline def pipeline = (a: Strom[Int], b: Strom[Int]) => body(a, b)

  inline def direct(a: Array[Int], b: Array[Int]) =
    body(Strom.fromArray(a), Strom.fromArray(b)).fold(0L)((s, x) => s * 31 + x)
