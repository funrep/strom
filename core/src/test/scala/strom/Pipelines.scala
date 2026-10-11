package strom

/** Pipelines shared by tests and benchmarks. Written as inline lambdas so that the same source
 *  is staged by the macros and interpreted by the reference interpreter. */
object Pipelines:
  inline def representative(n: Int) = (a: Strom[Int], b: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x)).zipWith(b.filter(y => y % 2 == 0))(_ * _).take(n).sum

  inline def representativeEmit(n: Int) = (a: Strom[Int], b: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x)).zipWith(b.filter(y => y % 2 == 0))(_ * _).take(n)

  inline def mapFilter = (a: Strom[Int]) =>
    a.map(x => x * 3 + 1).filter(x => x % 4 != 0).map(x => x - 7).filter(x => x > -10)

  inline def takeInside = (a: Strom[Int]) => a.flatMap(x => Strom.range(0, x % 7).take(3))
  inline def takeBefore = (a: Strom[Int]) => a.take(5).flatMap(x => Strom.range(0, x % 4))
  inline def takeAfterNested(n: Int) = (a: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x % 5)).filter(_ % 2 == 0).take(n)
  inline def zipPorts = (a: Strom[Int], b: Strom[Int]) => a.zipWith(b)(_ - _)
  inline def zipFilteredPorts = (a: Strom[Int], b: Strom[Int]) => a.filter(_ > 0).zipWith(b.filter(_ < 10))(_ * 100 + _)
  inline def zipRange = (a: Strom[Int]) => a.zipWith(Strom.range(0, 1000))(_ + _)
  inline def zipRangeLeft = (a: Strom[Int]) => Strom.range(5, 30).zipWith(a.filter(_ % 3 == 0))(_ * _)
  inline def window(k: Int) = (a: Strom[Int]) => a.filter(_ != 3).windowFold(k)(0)(_ + _)
  inline def windowNestedZip = (a: Strom[Int], b: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x % 4)).windowFold(2)(1)(_ * 2 + _).zipWith(b)(_ + _)
  inline def zipInsideFlatMap = (a: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x % 5).zipWith(Strom.range(10, 10 + x % 3))(_ * _))
  inline def longCount = (a: Strom[Int]) => a.map(_.toLong * 3_000_000_000L).filter(_ > 0).count
  inline def longSum = (a: Strom[Int]) => a.map(_.toLong * 3_000_000_000L).sum
  inline def doubleSum = (a: Strom[Int]) => a.map(_ * 0.5).filter(_ < 20).sum
  inline def strings = (a: Strom[Int]) => a.map(x => s"v$x").filter(_.length < 3)
  inline def tuples = (a: Strom[Int], b: Strom[Int]) => a.zip(b.map(_.toString))
  inline def threeLevels = (a: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x % 4).flatMap(y => Strom.range(y, x % 6).map(z => x * 100 + y * 10 + z)))
  inline def closureInLambda = (a: Strom[Int]) => a.map(x => List(1, 2, 3).count(_ < x % 4)).filter(_ > 0)
  inline def takeZero = (a: Strom[Int]) => a.map(_ + 1).take(0)
  inline def takeNegative = (a: Strom[Int]) => a.flatMap(x => Strom.range(0, x)).take(-3)
  inline def nonAssoc = (a: Strom[Int]) => a.fold(7)((s, x) => s * 31 + x)
  inline def foldTuple = (a: Strom[Int]) => a.fold((0, 0))((s, x) => (s._1 + x, s._2 max x))
  inline def windowZero = (a: Strom[Int]) => a.windowFold(0)(0L)(_ + _)
  inline def takeOneSide(n: Int) = (a: Strom[Int], b: Strom[Int]) =>
    a.take(n).zipWith(b.flatMap(y => Strom.range(0, y % 3)))(_ + _).filter(_ % 2 == 1)
  inline def flatMapArray(arr: Array[Int]) = (a: Strom[Int]) =>
    a.flatMap(x => Strom.fromArray(arr).map(_ * x).take(x % 4))
  inline def bytes = (a: Strom[Int]) => a.map(_.toByte).filter(_ > 3).windowFold(2)(0.toByte)((s, x) => (s + x).toByte)

  // ---- operators added for fs2 / ZIO coverage
  inline def collected = (a: Strom[Int]) => a.collect { case x if x % 3 == 0 => x * 10; case 7 => -7 }
  inline def collectTuple = (a: Strom[Int], b: Strom[Int]) =>
    a.zip(b).collect { case (x, y) if x < y => x - y }.filter(_ != -3)
  inline def optional = (a: Strom[Int]) => a.mapFilter(x => if x > 4 then Some(x.toLong * 2) else None)
  inline def indexed = (a: Strom[Int]) => a.zipWithIndex.map((x, i) => x * 1000 + i.toInt).filter(_ % 2 == 0)
  inline def scanned = (a: Strom[Int]) => a.filter(_ > 0).scan(0L)(_ + _).drop(2)
  inline def whiles = (a: Strom[Int]) => a.dropWhile(_ < 10).takeWhile(_ != 13).map(_ + 1)
  inline def nestedNew = (a: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x % 6).dropWhile(_ < 1).takeWhile(_ < 4).collect { case y if y != 2 => y * x })
      .scan(1)(_ + _).takeWhile(_ < 500)
  inline def zipScan = (a: Strom[Int], b: Strom[Int]) =>
    a.scan(0)(_ + _).zipWith(b.drop(3).zipWithIndex)((s, t) => s + t._1 + t._2.toInt)

  // ---- Mealy machines
  /** Run lengths of equal neighbours, emitted when the value changes (branches with Some / None). */
  inline def runLengths = (a: Strom[Int]) =>
    a.map(_ % 3).mapAccumFilter((-1, 0))((st, x) =>
      if st._1 == x then ((x, st._2 + 1), None)
      else ((x, 1), if st._1 < 0 then None else Some(st._2 * 10 + st._1)))
  inline def runningMax = (a: Strom[Int]) => a.mapAccum(Int.MinValue)((m, x) => (m max x, m max x)).filter(_ % 2 == 0)
  inline def accumInFlatMap = (a: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x % 5).mapAccum(0)((s, y) => (s + y, s * x))).take(60)

