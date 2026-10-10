package strom.zio

import scala.quoted.*
import scala.reflect.ClassTag
import _root_.zio.Trace
import _root_.zio.stream.ZStream
import strom.internal.{ChainFuser, Codegen, Pipe}
import strom.runtime.SegmentFactory

private[zio] object FusedMacro:
  def fused[S: Type](stream: Expr[S])(using Quotes): Expr[S] =
    ZioFuser().fuse(stream).asExprOf[S]

  def plan(stream: Expr[Any])(using Quotes): Expr[String] =
    val f = ZioFuser()
    f.fuse(stream)
    Expr(f.summary)

/** ZStream operators: `map`, `filter`, `take`, `flatMap` (onto `ZStream.range`). One input per run. */
private[zio] final class ZioFuser(using Quotes) extends ChainFuser:
  import q.reflect.*

  protected val streamClass = Symbol.requiredClass("zio.stream.ZStream")
  private val StreamModule = Symbol.requiredModule("zio.stream.ZStream")

  private def args(t: TypeRepr): List[TypeRepr] = t.widen.dealias match
    case AppliedType(_, as) => as
    case other => report.errorAndAbort(s"strom: unexpected stream type ${other.show}")
  protected def elemOf(t: TypeRepr): TypeRepr = args(t)(2)

  protected def maxInputs = 1
  protected def sourcesSupported = false

  // ZStream operators take an implicit Trace as their last argument list.
  protected def pureOp(c: Call): Option[PureOp] = (c.name, c.argss) match
    case ("map", List(f) :: _ :: Nil)     => Some(PureOp.Map(f))
    case ("filter", List(p) :: _ :: Nil)  => Some(PureOp.Filter(p))
    case ("take", List(n) :: _ :: Nil)    => Some(PureOp.Take(n))
    case ("flatMap", List(f) :: _ :: Nil) => Some(PureOp.FlatMap(f))
    case _ => None

  protected def intRange(t: Term): Option[(Expr[Int], Expr[Int])] = asCall(strip(t)) match
    case Some(Call(_, qual, "range", _, (a :: b :: _) :: _)) if qual.symbol == StreamModule =>
      Some((a.asExprOf[Int], b.asExprOf[Int]))
    case _ => None

  /** `input.via(StromZio.pipeline[A, B](factory))`, ascribed the type of the run's last call. */
  protected def emit(r: Run, sink: Pipe.Sink): Term =
    val last = r.calls.last.term
    val in = r.inputs.head._1
    val List(rT, eT, aT) = args(in.tpe): @unchecked
    val call = (rT.asType, eT.asType, aT.asType, elemOf(last.tpe).asType) match
      case ('[r], '[e], '[a], '[b]) =>
        val ct = Expr.summon[ClassTag[a]].getOrElse(report.errorAndAbort(s"strom: no ClassTag for ${aT.show}"))
        val factory = Codegen.segment[b](sink, List(Type.of[a]))
        '{ ${ in.asExprOf[ZStream[r, e, a]] }.via(StromZio.pipeline[a, b]($factory)(using $ct, summon[Trace])) }
    Typed(call.asTerm, Inferred(last.tpe))
