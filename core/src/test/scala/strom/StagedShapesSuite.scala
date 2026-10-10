package strom

import scala.quoted.*
import scala.util.Random
import strom.internal.{Codegen, Pipe}
import strom.runtime.{PlanInfo, Segment, SegmentFactory}

/** Property test over random pipeline *shapes*: each shape is compiled at test time with
 *  `scala.quoted.staging` (the same code generator the macros use), then run with random
 *  inputs and chunkings and compared with the reference interpreter. */
class StagedShapesSuite extends munit.FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(10, "min")

  // ------------------------------------------------------------------ shapes and their two meanings

  enum Shape:
    case Port(n: Int)
    case Range(lo: Int, hi: Int)
    /** Inner source of a flatMap: `range(x % a, x % b)` where x is the outer element. */
    case InnerRange(a: Int, b: Int)
    case Map(s: Shape, f: Int)
    case Filter(s: Shape, p: Int)
    case FlatMap(s: Shape, inner: Shape)
    case Take(s: Shape, n: Int)
    case Zip(l: Shape, r: Shape, f: Int)
    case Window(s: Shape, size: Int)

  import Shape.*

  val maps: Vector[(Int => Int, Expr[Int] => Quotes ?=> Expr[Int])] = Vector(
    (x => x * 3 + 1, x => '{ $x * 3 + 1 }),
    (x => x - 7, x => '{ $x - 7 }),
    (x => x / 2, x => '{ $x / 2 }),
    (x => x ^ 5, x => '{ $x ^ 5 }),
    (x => x % 13, x => '{ $x % 13 }),
  )
  val preds: Vector[(Int => Boolean, Expr[Int] => Quotes ?=> Expr[Boolean])] = Vector(
    (x => x % 3 != 0, x => '{ $x % 3 != 0 }),
    (x => x > 0, x => '{ $x > 0 }),
    (x => x % 2 == 0, x => '{ $x % 2 == 0 }),
    (x => x < 20, x => '{ $x < 20 }),
  )
  val zips: Vector[((Int, Int) => Int, (Expr[Int], Expr[Int]) => Quotes ?=> Expr[Int])] = Vector(
    (_ + _, (x, y) => '{ $x + $y }),
    (_ * _, (x, y) => '{ $x * $y }),
    (_ - _, (x, y) => '{ $x - $y }),
  )

  def reference(s: Shape, ports: Vector[Seq[Int]], x: Int = 0): Strom[Int] = s match
    case Port(n)          => Strom.fromSeq(ports(n))
    case Range(lo, hi)    => Strom.range(lo, hi)
    case InnerRange(a, b) => Strom.range(x % a, x % b)
    case Map(u, f)        => reference(u, ports, x).map(maps(f)._1)
    case Filter(u, p)     => reference(u, ports, x).filter(preds(p)._1)
    case FlatMap(u, in)   => reference(u, ports, x).flatMap(y => reference(in, ports, y))
    case Take(u, n)       => reference(u, ports, x).take(n)
    case Zip(l, r, f)     => reference(l, ports, x).zipWith(reference(r, ports, x))(zips(f)._1)
    case Window(u, k)     => reference(u, ports, x).windowFold(k)(0)(_ + _)

  def staged(s: Shape, x: Option[Expr[Int]])(using Quotes): Pipe[Int] = s match
    case Port(n)          => Pipe.SrcPort[Int](n)
    case Range(lo, hi)    => Pipe.SrcRange(Expr(lo), Expr(hi))
    case InnerRange(a, b) => Pipe.SrcRange('{ ${ x.get } % ${ Expr(a) } }, '{ ${ x.get } % ${ Expr(b) } })
    case Map(u, f)        => Pipe.MapP[Int, Int](staged(u, x), Pipe.Fn1(e => maps(f)._2(e)))
    case Filter(u, p)     => Pipe.FilterP[Int](staged(u, x), Pipe.Fn1(e => preds(p)._2(e)))
    case FlatMap(u, in)   => Pipe.FlatMapP[Int, Int](staged(u, x), Pipe.Inner(y => staged(in, Some(y))))
    case Take(u, n)       => Pipe.TakeP[Int](staged(u, x), Expr(n.toLong))
    case Zip(l, r, f)     => Pipe.ZipWithP[Int, Int, Int](staged(l, x), staged(r, x), Pipe.Fn2((a, b) => zips(f)._2(a, b)))
    case Window(u, k)     => Pipe.WindowFoldP[Int, Int](staged(u, x), Expr(k), '{ 0 }, Pipe.Fn2((a, b) => '{ $a + $b }))

  // ------------------------------------------------------------------ generator

  /** A random shape consuming the given ports exactly once each (inner shapes use no ports). */
  def gen(r: Random, ports: List[Int], depth: Int, inner: Boolean): Shape =
    def leaf(ps: List[Int]): Shape = ps match
      case p :: Nil => Port(p)
      case Nil      => if inner then InnerRange(2 + r.nextInt(3), 3 + r.nextInt(6)) else Range(r.nextInt(5), r.nextInt(40))
      case _        => sys.error("leaf with several ports")
    if depth == 0 && ports.size <= 1 then leaf(ports)
    else
      val choice = if ports.size == 2 then 5 else r.nextInt(8)
      def again(ps: List[Int]) = gen(r, ps, math.max(depth - 1, 0), inner)
      choice match
        case 0 => Map(again(ports), r.nextInt(maps.size))
        case 1 => Filter(again(ports), r.nextInt(preds.size))
        case 2 => FlatMap(again(ports), gen(r, Nil, math.min(depth, 2), inner = true))
        case 3 => Take(again(ports), r.nextInt(12) - 1)
        case 4 => Window(again(ports), r.nextInt(4))
        case 5 | 6 =>
          val (lp, rp) = ports match
            case List(a, b) => (List(a), List(b))
            case ps         => if r.nextBoolean() then (ps, Nil) else (Nil, ps)
          Zip(gen(r, lp, math.max(depth - 1, 0), inner), gen(r, rp, math.max(depth - 1, 0), inner), r.nextInt(zips.size))
        case _ => if ports.size <= 1 then leaf(ports) else again(ports)

  final case class Case(shape: Shape, inputs: Int, fold: Boolean)

  def randomCase(r: Random): Case =
    val inputs = 1 + r.nextInt(2)
    Case(gen(r, List.range(0, inputs), 1 + r.nextInt(4), inner = false), inputs, r.nextBoolean())

  // ------------------------------------------------------------------ staging

  given staging.Compiler = staging.Compiler.make(getClass.getClassLoader)

  def compile(cases: Seq[Case]): List[SegmentFactory[Int]] =
    val makers = staging.run {
      val segs = cases.toList.map { c =>
        val up = staged(c.shape, None)
        val sink = if c.fold then Pipe.FoldSink[Int, Int](up, '{ 0 }, Pipe.Fn2((a, b) => '{ $a + $b }), true)
                   else Pipe.EmitSink(up)
        Codegen.segmentClass[Int](sink, List.fill(c.inputs)(Type.of[Int]))
      }
      Expr.ofList(segs)
    }
    cases.toList.zip(makers).map((c, mk) => SegmentFactory[Int](c.inputs, c.fold, PlanInfo(c.inputs, Nil, c.fold), mk))

  // ------------------------------------------------------------------ the property

  private val batches = sys.env.get("STROM_SHAPE_BATCHES").map(_.toInt).getOrElse(8)

  for batch <- 0 until batches do
    test(s"random shapes, batch $batch") {
      val r = Random(1000 + batch)
      val cases = Seq.fill(25)(randomCase(r))
      val factories = compile(cases)
      for (c, f) <- cases.zip(factories); i <- 0 until 40 do
        val ins = Vector.fill(c.inputs)(Array.fill(r.nextInt(if i % 8 == 0 then 120 else 20))(r.nextInt(60) - 15))
        val cfg = Harness.randomConfig(r)
        val ctx = s"shape=${c.shape} fold=${c.fold} inputs=${ins.map(_.mkString("[", ",", "]"))} $cfg"
        val ref = reference(c.shape, ins.map(_.toSeq))
        val got =
          try Harness.drive(f, ins, cfg)
          catch case e: Throwable => fail(s"segment failed: $e\n$ctx", e)
        if c.fold then assertEquals(got.result, Some(Reference.run(ref.sum)), ctx)
        else assertEquals(got.outputs.toList, Reference.toList(ref), ctx)
    }
