package strom.fs2

import strom.runtime.Segment
import scala.quoted.*
import scala.reflect.ClassTag
import strom.internal.{ChainFuser, Codegen, Pipe}

private[fs2] object FusedMacro:
  def fused[S: Type](stream: Expr[S])(using Quotes): Expr[S] =
    Fs2Fuser().fuse(stream).asExprOf[S]

  /** What `fused` does to an expression: one line per fused run. */
  def plan(stream: Expr[Any])(using Quotes): Expr[String] =
    val f = Fs2Fuser()
    f.fuse(stream)
    Expr(f.summary)

/** fs2 operators: `map`, `filter`, `take`, `fold`, `flatMap` (onto `Stream.range`), `zipWith`, `collect`,
 *  `mapFilter` (cats syntax), `zipWithIndex`, `scan`, `drop`, `takeWhile`, `dropWhile`. */
private[fs2] final class Fs2Fuser(using Quotes) extends ChainFuser:
  import q.reflect.*

  protected val streamClass = Symbol.requiredClass("fs2.Stream")
  private val StreamModule = Symbol.requiredModule("fs2.Stream")
  private val verbose = System.getProperty("strom.verbose") != null

  private def args(t: TypeRepr): List[TypeRepr] = t.widen.dealias match
    case AppliedType(_, as) => as
    case other => report.errorAndAbort(s"strom: unexpected stream type ${other.show}")
  protected def elemOf(t: TypeRepr): TypeRepr = args(t)(1)
  private def effectOf(t: TypeRepr): TypeRepr = args(t)(0)

  protected def maxInputs = strom.runtime.Segment.MaxPorts
  protected def sourcesSupported = true

  protected def pureOp(c: Call): Option[PureOp] = (c.name, c.argss) match
    case ("map", List(List(f)))                => Some(PureOp.Map(f))
    case ("filter", List(List(p)))             => Some(PureOp.Filter(p))
    case ("take", List(List(n)))               => Some(PureOp.Take(n))
    case ("fold", List(List(z), List(f)))      => Some(PureOp.Fold(z, f))
    case ("collect", List(List(pf)))           => Some(PureOp.Collect(pf))
    case ("mapFilter*", List(List(f)))         => Some(PureOp.MapOption(f)) // cats FunctorFilter syntax
    case ("zipWithIndex", Nil)                 => Some(PureOp.ZipWithIndex())
    case ("zip", List(List(that)))             => Some(PureOp.Zip(that))
    case ("scan", List(List(z), List(f)))      => Some(PureOp.Scan(z, f))
    case ("drop", List(List(n)))               => Some(PureOp.Drop(n))
    case ("dropWhile", List(List(p)))          => Some(PureOp.DropWhile(p))
    // takeWhile(p, takeFailure = false) only
    case ("takeWhile", List(List(p, flag))) if isFalse(flag) => Some(PureOp.TakeWhile(p))
    case ("flatMap", List(f) :: _)             => Some(PureOp.FlatMap(f))
    // zipWith[F2, O2 >: O, O3, O4]: only without widening of the left element type.
    case ("zipWith", List(List(that), List(f))) if c.targs.size == 4 && c.targs(1) =:= elemOf(c.qual.tpe) =>
      Some(PureOp.ZipWith(that, f))
    case _ => None

  private def isFalse(t: Term): Boolean = strip(t) match
    case Literal(BooleanConstant(false)) => true
    case other => other.symbol.name.contains("$default$") // the default argument (false)

  protected def intRange(t: Term): Option[(Expr[Int], Expr[Int])] = asCall(strip(t)) match
    case Some(Call(_, qual, "range", _, List(a, b) :: _)) if qual.symbol == StreamModule && elemOf(t.tpe) =:= TypeRepr.of[Int] =>
      Some((a.asExprOf[Int], b.asExprOf[Int]))
    case _ => None

  /** `StromFs2.fusedRun[F, B](factory, inputs, tags, cede, maxOut, budget)` */
  protected def emit(r: Run, sink: Pipe.Sink): Term =
    val last = r.calls.last.term
    val (fRepr, bRepr) = (effectOf(last.tpe), elemOf(last.tpe))
    val factory = bRepr.asType match
      case '[b] => Codegen.segment[b](sink, r.inputs.toList.map(_._2.asType))
    val inputs = '{ Vector[Any](${ Varargs(r.inputs.map(_._1.asExpr)) }*) }
    val tags = '{ Vector[ClassTag[?]](${ Varargs(r.inputs.map((_, t) => classTag(t))) }*) }
    val spawn = Symbol.requiredClass("cats.effect.kernel.GenSpawn").typeRef.appliedTo(List(fRepr, TypeRepr.of[Throwable]))
    val (cede, cedes) = Implicits.search(spawn) match
      case s: ImplicitSearchSuccess => (Select.unique(s.tree, "cede"), true)
      case _: ImplicitSearchFailure => ('{ null }.asTerm, false)
    if !cedes then log(log.size - 1) += ", never cedes"
    if verbose then report.info(s"strom: ${log.last}", last.pos)
    Select.unique(Ref(Symbol.requiredModule("strom.fs2.StromFs2")), "fusedRun").appliedToTypes(List(fRepr, bRepr))
      .appliedToArgs(List(factory.asTerm, inputs.asTerm, tags.asTerm, cede, Literal(IntConstant(strom.runtime.Segment.DefaultMaxOut)), Literal(IntConstant(strom.runtime.Segment.DefaultBudget))))

  private def classTag(t: TypeRepr): Expr[ClassTag[?]] = t.asType match
    case '[a] => Expr.summon[ClassTag[a]].getOrElse(report.errorAndAbort(s"strom: no ClassTag for ${t.show}"))
