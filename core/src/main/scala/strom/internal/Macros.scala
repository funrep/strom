package strom.internal

import scala.quoted.*
import strom.runtime.SegmentFactory

object Macros:

  /** Turns generator limits into compile errors at the call site. */
  private def guarded[T](body: => T)(using Quotes): T =
    try body
    catch case e: StromError => quotes.reflect.report.errorAndAbort(s"strom: ${e.getMessage}")

  def run[R: Type](fold: Expr[strom.Fold[R]])(using Quotes): Expr[R] =
    Frontend().parseSink(fold) match
      case fs: Pipe.FoldSink[?, ?] => guarded(Codegen.direct(fs).asExprOf[R])
      case _ => quotes.reflect.report.errorAndAbort("Strom.run expects a fold")

  private def sinkType(sink: Pipe.Sink): Type[?] = sink match
    case fs: Pipe.FoldSink[?, ?] => fs.tpe
    case es: Pipe.EmitSink[?]    => es.tpe

  def showCode(p: Expr[Any])(using Quotes): Expr[String] =
    val code = Frontend().segmentLambda(p) match
      case Some((ports, sink)) =>
        sinkType(sink) match
          case '[o] =>
            val legend = collection.mutable.Buffer.empty[(String, String)]
            val code = Codegen.segmentClass[o](sink, ports, legend).show
            val slots = legend.map((s, h) => s"$s=$h").mkString(" ")
            s"// state slots: $slots\n$code"
      case None =>
        Frontend().parseSink(p) match
          case fs: Pipe.FoldSink[?, ?] => Codegen.direct(fs).show
          case _ => quotes.reflect.report.errorAndAbort("strom: showCode expects a fold or a segment lambda")
    Expr(code)

  def explain(p: Expr[Any])(using Quotes): Expr[String] =
    val text = Frontend().segmentLambda(p) match
      case Some((ports, sink)) => Codegen.explain(sink, ports)
      case None => Codegen.explain(Frontend().parseSink(p), Nil)
    Expr(text)

  def parallelKeyed[O: Type](f: Expr[Any], key: Expr[Any])(using Quotes): Expr[strom.runtime.ParallelPlan[O]] =
    parallelPlan[O](f, Some(key))

  private def parallelPlan[O: Type](f: Expr[Any], key: Option[Expr[Any]])(using Quotes): Expr[strom.runtime.ParallelPlan[O]] =
    val front = Frontend()
    front.segmentLambda(f) match
      case Some((ports, sink)) => guarded(Parallel.plan[O](sink, ports, key, front))
      case None => quotes.reflect.report.errorAndAbort("strom: expected a lambda literal")

  def segmentOf[R: Type](f: Expr[Any])(using Quotes): Expr[strom.runtime.SegmentFactory[strom.Strom.ResultOf[R]]] =
    Frontend().segmentLambda(f) match
      case Some((ports, sink)) => sinkType(sink) match
        case '[o] => guarded(Codegen.segment[o](sink, ports))
          .asInstanceOf[Expr[strom.runtime.SegmentFactory[strom.Strom.ResultOf[R]]]]
      case None => quotes.reflect.report.errorAndAbort("strom: expected a lambda literal with typed Strom parameters")

  def parallelOf[R: Type](f: Expr[Any])(using Quotes): Expr[strom.runtime.ParallelPlan[strom.Strom.ResultOf[R]]] =
    val front = Frontend()
    front.segmentLambda(f) match
      case Some((ports, sink)) => sinkType(sink) match
        case '[o] => guarded(Parallel.plan[o](sink, ports, None, front))
          .asInstanceOf[Expr[strom.runtime.ParallelPlan[strom.Strom.ResultOf[R]]]]
      case None => quotes.reflect.report.errorAndAbort("strom: expected a lambda literal with a typed Strom parameter")
