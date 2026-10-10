package strom.zio

import scala.quoted.*
import scala.reflect.ClassTag
import _root_.zio.Trace
import _root_.zio.stream.ZStream
import strom.internal.{ChainFuser, Codegen, Pipe}

private[zio] object FusedMacro:
  def fused[S: Type](stream: Expr[S])(using Quotes): Expr[S] =
    ZioFuser().fuse(stream).asExprOf[S]

  def plan(stream: Expr[Any])(using Quotes): Expr[String] =
    val f = ZioFuser()
    f.fuse(stream)
    Expr(f.summary)

/** ZStream operators: `map`, `filter`, `take`, `flatMap` (onto `ZStream.range`), `zipWith`, `collect`,
 *  `zipWithIndex`, `scan`, `drop`, `takeWhile`, `dropWhile`. */
private[zio] final class ZioFuser(using Quotes) extends ChainFuser:
  import q.reflect.*

  protected val streamClass = Symbol.requiredClass("zio.stream.ZStream")
  private val StreamModule = Symbol.requiredModule("zio.stream.ZStream")

  private def args(t: TypeRepr): List[TypeRepr] = t.widen.dealias match
    case AppliedType(_, as) => as
    case other => report.errorAndAbort(s"strom: unexpected stream type ${other.show}")
  protected def elemOf(t: TypeRepr): TypeRepr = args(t)(2)

  protected def maxInputs = strom.runtime.Segment.MaxPorts
  protected def sourcesSupported = true

  // ZStream operators take an implicit Trace as their last argument list.
  protected def pureOp(c: Call): Option[PureOp] = (c.name, c.argss) match
    case ("map", List(f) :: _ :: Nil)     => Some(PureOp.Map(f))
    case ("filter", List(p) :: _ :: Nil)  => Some(PureOp.Filter(p))
    case ("take", List(n) :: _ :: Nil)    => Some(PureOp.Take(n))
    case ("flatMap", List(f) :: _ :: Nil) => Some(PureOp.FlatMap(f))
    case ("zipWith", List(that) :: List(f) :: _ :: Nil) => Some(PureOp.ZipWith(that, f))
    case ("collect", List(pf) :: _ :: Nil) => Some(PureOp.Collect(pf))
    case ("zipWithIndex", List(_) :: Nil) => Some(PureOp.ZipWithIndex())
    case ("scan", List(z) :: List(f) :: _ :: Nil) => Some(PureOp.Scan(z, f))
    case ("drop", List(n) :: _ :: Nil) => Some(PureOp.Drop('{ ${ n.asExprOf[Int] }.toLong }.asTerm))
    case ("takeWhile", List(p) :: _ :: Nil) => Some(PureOp.TakeWhile(p))
    case ("dropWhile", List(p) :: _ :: Nil) => Some(PureOp.DropWhile(p))
    case _ => None

  protected def intRange(t: Term): Option[(Expr[Int], Expr[Int])] = asCall(strip(t)) match
    case Some(Call(_, qual, "range", _, (a :: b :: _) :: _)) if qual.symbol == StreamModule =>
      Some((a.asExprOf[Int], b.asExprOf[Int]))
    case _ => None

  /** `StromZio.fusedRun[R, E, B](factory, inputs, tags, maxOut, budget)`, ascribed the type of
   *  the run's last call. */
  protected def emit(r: Run, sink: Pipe.Sink): Term =
    val last = r.calls.last.term
    val List(rT, eT, bT) = args(last.tpe): @unchecked
    val factory = bT.asType match
      case '[b] => Codegen.segment[b](sink, r.inputs.toList.map(_._2.asType))
    val inputs = '{ Vector[Any](${ Varargs(r.inputs.map(_._1.asExpr)) }*) }
    val tags = '{ Vector[ClassTag[?]](${ Varargs(r.inputs.map((_, t) => classTag(t))) }*) }
    val trace = Expr.summon[Trace].getOrElse(report.errorAndAbort("strom: no zio.Trace"))
    val call = Select.unique(Ref(Symbol.requiredModule("strom.zio.StromZio")), "fusedRun").appliedToTypes(List(rT, eT, bT))
      .appliedToArgs(List(factory.asTerm, inputs.asTerm, tags.asTerm, Literal(IntConstant(1024)), Literal(IntConstant(4096))))
      .appliedToArgs(List(trace.asTerm))
    Typed(call, Inferred(last.tpe))

  private def classTag(t: TypeRepr): Expr[ClassTag[?]] = t.asType match
    case '[a] => Expr.summon[ClassTag[a]].getOrElse(report.errorAndAbort(s"strom: no ClassTag for ${t.show}"))
