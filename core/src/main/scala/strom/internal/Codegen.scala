package strom.internal

import scala.quoted.*
import strom.runtime.{DirectFrame, PlanInfo, Segment, SegmentFactory, Slots}

/** Turns a [[Pipe.Sink]] into code: a direct fused loop, or a resumable [[strom.runtime.Segment]] class.
 *  Shared by the macros (compile time) and by `scala.quoted.staging` (tests). */
object Codegen:

  /** Runs `build` once; if the result is too large for one JIT-able method, runs it again
   *  with the regions chosen by [[Gen.choose]] outlined into separate methods. */
  private def withSplitting[R](storage: Storage)(build: Gen.SplitPlan => (Gen, Expr[R]))(using Quotes): Expr[R] =
    storage.reset()
    val first = Gen.SplitPlan(Set.empty)
    val (g, e) = build(first)
    val total = g.treeSize(e)
    if System.getProperty("strom.debug") != null then
      println(s"[strom] tree nodes=$total estimated bytes=${(total * Gen.BytesPerNode).toInt}")
    if total * Gen.BytesPerNode <= Gen.MethodBudgetBytes then e
    else
      val outline = Gen.choose(total, first.sizes, first.parent)
      storage.reset()
      build(Gen.SplitPlan(outline))._2

  private def enclosingMethod(using Quotes): quotes.reflect.Symbol =
    import quotes.reflect.*
    var o = Symbol.spliceOwner
    while !o.isDefDef do o = o.owner
    o

  // ------------------------------------------------------------------ direct mode

  def direct[S](sink: Pipe.FoldSink[?, S])(using Quotes): Expr[S] =
    import quotes.reflect.*
    given Type[S] = sink.tpe
    val owner = Symbol.spliceOwner
    val storage = LocalStorage(owner)
    storage.reset()
    val g = Gen(storage, Gen.Mode.Direct, Gen.SplitPlan(Set.empty), owner)
    val e = storage.wrap(g.directFold(sink))
    if g.treeSize(e) * Gen.BytesPerNode <= Gen.MethodBudgetBytes then e
    else
      // Too large for one method: run on a one-shot frame with field storage and split methods.
      '{ (new DirectFrame[S] { def runImpl(): S = ${ framed(sink) } }).runImpl() }

  private def framed[S](sink: Pipe.FoldSink[?, S])(using Quotes): Expr[S] =
    import quotes.reflect.*
    given Type[S] = sink.tpe
    val meth = enclosingMethod
    val self = This(meth.owner)
    val storage = FieldStorage(self.asExprOf[Slots])
    withSplitting(storage) { plan =>
      val g = Gen(storage, Gen.Mode.Direct, plan, meth)
      (g, g.directFold(sink))
    }

  // ------------------------------------------------------------------ resumable mode

  /** A factory expression for the segment computing `sink` from `portTypes.size` inputs. */
  def segment[O: Type](sink: Pipe.Sink, portTypes: List[Type[?]])(using Quotes): Expr[SegmentFactory[O]] =
    import Pipe.given
    val plan = Expr(Pipe.planInfo(sink, portTypes.size))
    '{ new SegmentFactory[O](${ Expr(portTypes.size) }, ${ Expr(isFold(sink)) }, $plan, ${ segmentClass[O](sink, portTypes) }) }

  private def isFold(sink: Pipe.Sink) = sink.isInstanceOf[Pipe.FoldSink[?, ?]]

  /** `(maxOut, budget) => new Segment { … }`. `legend` receives the slot assignment. */
  def segmentClass[O: Type](sink: Pipe.Sink, portTypes: List[Type[?]],
                            legend: collection.mutable.Buffer[(String, String)] = collection.mutable.Buffer.empty)(
      using Quotes): Expr[(Int, Int) => Segment] =
    val n = Expr(portTypes.size)
    val isFoldE = Expr(isFold(sink))
    '{ (cap: Int, budget0: Int) =>
      val seg = new Segment($n, cap, budget0) {
        def isFold: Boolean = $isFoldE
        protected def stepImpl(): Int = ${ stepBody(sink, portTypes, legend) }
      }
      seg.out = ${ if isFold(sink) then '{ null } else newArray[O]('cap) }
      seg
    }

  /** The output buffer: a primitive array chosen from the element type. */
  def newArray[O: Type](size: Expr[Int])(using Quotes): Expr[AnyRef] = Type.of[O] match
    case '[Int]     => '{ new Array[Int]($size) }
    case '[Long]    => '{ new Array[Long]($size) }
    case '[Double]  => '{ new Array[Double]($size) }
    case '[Float]   => '{ new Array[Float]($size) }
    case '[Boolean] => '{ new Array[Boolean]($size) }
    case '[Byte]    => '{ new Array[Byte]($size) }
    case '[Short]   => '{ new Array[Short]($size) }
    case '[Char]    => '{ new Array[Char]($size) }
    case _ =>
      Expr.summon[scala.reflect.ClassTag[O]] match
        case Some(ct) => '{ $ct.newArray($size).asInstanceOf[AnyRef] }
        case None     => quotes.reflect.report.errorAndAbort(s"strom: no ClassTag for output type ${Type.show[O]}")

  /** The body of `stepImpl`: casts the port and output arrays to their element types once,
   *  then runs the generated resumable loops. */
  def stepBody(sink: Pipe.Sink, portTypes: List[Type[?]], legend: collection.mutable.Buffer[(String, String)])(
      using Quotes): Expr[Int] =
    import quotes.reflect.*
    val meth = enclosingMethod
    val self = This(meth.owner)
    def local(name: String, field: String, elem: Type[?]): (ValDef, Expr[Array[?]]) =
      elem match
        case '[t] =>
          val tpe = TypeRepr.of[Array[t]]
          val sym = Symbol.newVal(meth, name, tpe, Flags.EmptyFlags, Symbol.noSymbol)
          val rhs = '{ ${ Select.unique(self, field).asExprOf[AnyRef] }.asInstanceOf[Array[t]] }
          (ValDef(sym, Some(rhs.asTerm)), Ref(sym).asExprOf[Array[t]])
    val ports = portTypes.zipWithIndex.map((t, i) => local(s"in$i", s"in$i", t))
    val outLocal = sink match
      case e: Pipe.EmitSink[?] => Some(local("out", "out", e.tpe))
      case _                   => None
    val storage = FieldStorage(self.asExprOf[Slots])
    val mode = Gen.Mode.Resumable(self, ports.map(_._2).toVector, outLocal.map(_._2).getOrElse('{ null }))
    val body = withSplitting(storage) { plan =>
      val g = Gen(storage, mode, plan, meth)
      (g, g.segmentStep(sink))
    }
    legend.clear()
    legend ++= storage.legend
    Block(ports.map(_._1) ++ outLocal.map(_._1), body.asTerm).asExprOf[Int]

  // ------------------------------------------------------------------ explain

  def explain(sink: Pipe.Sink, portTypes: List[Type[?]])(using Quotes): String =
    val inputs = portTypes.size
    import quotes.reflect.*
    val owner = Symbol.spliceOwner
    val storage = LocalStorage(owner)
    val dummySelf = '{ null.asInstanceOf[Segment] }.asTerm
    val ports = portTypes.map { case '[t] => '{ null.asInstanceOf[Array[t]] } }.toVector
    val mode = Gen.Mode.Resumable(dummySelf, ports, '{ null })
    val g = Gen(storage, mode, Gen.SplitPlan(Set.empty), owner, describing = true)
    val shape = StStream.shape(g.lower(Pipe.upstream(sink)))
    val plan = Pipe.planInfo(sink, inputs)
    s"""stages:   $plan
       |stateless: ${plan.stateless}   associativeFold: ${plan.associativeFold}
       |IR:       $shape""".stripMargin
