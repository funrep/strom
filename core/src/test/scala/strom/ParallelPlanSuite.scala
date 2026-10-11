package strom

import scala.util.Random
import strom.runtime.*

/** The semantics of parallel plans, checked with a sequential executor that nevertheless uses
 *  several instances per tier (round-robin), like N workers would. */
class ParallelPlanSuite extends munit.FunSuite:

  final case class Out(elements: List[Any], result: Option[Any])

  def execute(plan: ParallelPlan[?], input: Seq[ArrayChunk], r: Random, workers: Int = 3): Out =
    def mk(f: SegmentFactory[?]) = f.create(1 + r.nextInt(8), 1 + r.nextInt(50))
    var chunks: Seq[ArrayChunk] = input
    var result: Option[Any] = None
    for tier <- plan.tiers do tier match
      case Tier.Replicated(f) =>
        val segs = Vector.fill(workers)(mk(f))
        chunks = chunks.zipWithIndex.map((c, i) => Chunks.processStateless(segs(i % workers), c))
      case Tier.PartialFold(f, combine) =>
        val segs = Vector.fill(workers)(mk(f))
        val partials = chunks.zipWithIndex.map((c, i) => Chunks.partialFold(segs(i % workers), c))
        result = Some(if partials.isEmpty then Chunks.end(mk(f))._2 else partials.reduce(combine))
        chunks = Nil
      case Tier.Single(f) =>
        val seg = mk(f)
        val outs = chunks.map(c => Chunks.feed(seg, c)._1)
        val (last, res) = Chunks.end(seg)
        chunks = outs :+ last
        if f.isFold then result = Some(res)
      case Tier.Keyed(f, part) =>
        val segs = Vector.fill(workers)(mk(f))
        val outs = chunks.flatMap { c =>
          val parts = part.route(c.array, c.offset, c.length, workers)
          segs.indices.map(p => Chunks.feed(segs(p), ArrayChunk(parts(p), 0, java.lang.reflect.Array.getLength(parts(p))))._1)
        }
        chunks = outs ++ segs.map(s => Chunks.end(s)._1)
    Out(chunks.toList.flatMap(_.elements), result)

  def chunk(xs: Array[Int], r: Random): Seq[ArrayChunk] =
    val b = Seq.newBuilder[ArrayChunk]
    var i = 0
    while i < xs.length do
      val n = math.min(xs.length - i, r.nextInt(8))
      b += ArrayChunk(xs, i, n); i += n
    b.result()

  def check(name: String, plan: ParallelPlan[?], tiers: String, ref: Seq[Int] => Out): Unit =
    test(name) {
      assertEquals(plan.tiers.map(_.productPrefix).mkString(","), tiers)
      val r = Random(name.hashCode)
      for i <- 0 until 300 do
        val xs = Array.fill(r.nextInt(60))(r.nextInt(50) - 10)
        assertEquals(execute(plan, chunk(xs, r), r), ref(xs.toSeq), s"iteration $i: ${xs.mkString(",")}")
    }

  def emits[A](s: Strom[A]) = Out(Reference.toList(s), None)
  def folds[A](f: Fold[A]) = Out(Nil, Some(Reference.run(f)))

  inline def p1 = (a: Strom[Int]) => a.map(_ * 3).filter(_ % 2 == 0).flatMap(x => Strom.range(0, x % 4))
  check("stateless pipeline: replicated", Strom.parallel(p1), "Replicated", xs => emits(p1(Strom.fromSeq(xs))))

  inline def p2 = (a: Strom[Int]) => a.flatMap(x => Strom.range(0, x % 5)).filter(_ != 2).take(40).map(_ + 1)
  check("cut at take", Strom.parallel(p2), "Replicated,Single", xs => emits(p2(Strom.fromSeq(xs))))

  inline def p3 = (a: Strom[Int]) => a.map(_ * 3).filter(_ > 0).sum
  check("associative sum: partial folds", Strom.parallel(p3), "PartialFold", xs => folds(p3(Strom.fromSeq(xs))))

  inline def p4 = (a: Strom[Int]) => a.filter(_ % 3 != 0).count
  check("count: partial folds", Strom.parallel(p4), "PartialFold", xs => folds(p4(Strom.fromSeq(xs))))

  inline def p5 = (a: Strom[Int]) => a.map(_ + 1).foldAssociative(1L)((s, x) => s * (x % 3 + 1))
  check("associative fold with element ≠ result type: cut", Strom.parallel(p5), "Replicated,Single", xs => folds(p5(Strom.fromSeq(xs))))

  inline def p6 = (a: Strom[Int]) => a.map(_ * 2).fold(7)((s, x) => s * 31 + x)
  check("non-associative fold: cut", Strom.parallel(p6), "Replicated,Single", xs => folds(p6(Strom.fromSeq(xs))))

  inline def p7 = (a: Strom[Int]) => a.take(9).map(_ * 2)
  check("stateful first: single", Strom.parallel(p7), "Single", xs => emits(p7(Strom.fromSeq(xs))))

  inline def p8 = (a: Strom[Int]) => a.map(_ - 1).zipWith(Strom.range(0, 1000))(_ * _).filter(_ % 2 == 0)
  check("cut at zip", Strom.parallel(p8), "Replicated,Single", xs => emits(p8(Strom.fromSeq(xs))))

  inline def p9 = (a: Strom[Int]) => a.filter(_ > 0).windowFold(3)(0)(_ + _)
  check("cut at window", Strom.parallel(p9), "Replicated,Single", xs => emits(p9(Strom.fromSeq(xs))))

  inline def p11 = (a: Strom[Int]) =>
    a.flatMap(x => Strom.range(0, x % 4)).take(60).map(_ * 3).filter(_ % 2 == 0).flatMap(x => Strom.range(0, x % 3)).map(_ + 1)
  check("multi-cut: replicated | take | replicated", Strom.parallel(p11), "Replicated,Single,Replicated",
    xs => emits(p11(Strom.fromSeq(xs))))

  inline def p12 = (a: Strom[Int]) => a.map(_ + 1).take(50).flatMap(x => Strom.range(0, x % 5)).map(_ * 2).sum
  check("multi-cut ending in a partial fold", Strom.parallel(p12), "Replicated,Single,PartialFold",
    xs => folds(p12(Strom.fromSeq(xs))))

  inline def p13 = (a: Strom[Int]) => a.map(_ + 1).take(50).map(_ * 2).drop(3).filter(_ > 4).windowFold(2)(0)(_ + _)
  check("light stateless runs stay fused; adjacent single tiers merge", Strom.parallel(p13), "Replicated,Single",
    xs => emits(p13(Strom.fromSeq(xs))))

  inline def p10 = (a: Strom[Int]) => a.windowFold(2)(0)(_ * 10 + _)
  test("keyed: per-partition instances, same multiset as running the pipeline per partition") {
    val plan = Strom.parallelKeyed((x: Int) => x % 7)(p10)
    assertEquals(plan.tiers.map(_.productPrefix).mkString(","), "Keyed")
    val r = Random(10)
    for _ <- 0 until 300 do
      val xs = Array.fill(r.nextInt(60))(r.nextInt(50))
      val workers = 1 + r.nextInt(4)
      val got = execute(plan, chunk(xs, r), r, workers).elements
      val expected = (0 until workers).flatMap(p => Reference.toList(p10(Strom.fromSeq(xs.toSeq.filter(x => KeyPartitioner.partition(x % 7, workers) == p)))))
      assertEquals(got.sortBy(_.asInstanceOf[Int]), expected.toList.sorted)
  }

  test("plan descriptions") {
    println(Strom.parallel(p2))
    println(Strom.parallel(p3))
  }
