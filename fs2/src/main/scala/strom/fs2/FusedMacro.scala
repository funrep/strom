package strom.fs2

import strom.runtime.Segment
import scala.quoted.*
import strom.internal.{ChainFuser, Pipe}

private[fs2] object FusedMacro:
  def fused[S: Type](stream: Expr[S])(using Quotes): Expr[S] =
    Fs2Fuser().fuse(stream).asExprOf[S]

  /** What `fused` does to an expression: one line per fused run. */
  def plan(stream: Expr[Any])(using Quotes): Expr[String] =
    val f = Fs2Fuser()
    f.fuse(stream)
    Expr(f.summary)

  def fusedPar[S: Type](parallelism: Expr[Int], stream: Expr[S])(using Quotes): Expr[S] =
    Fs2Fuser(Some(parallelism)).fuse(stream).asExprOf[S]

  def parPlan(stream: Expr[Any])(using Quotes): Expr[String] =
    val f = Fs2Fuser(Some('{ 1 }))
    f.fuse(stream)
    Expr(f.summary)

/** fs2 operators: `map`, `filter`, `take`, `fold`, `flatMap` (onto `Stream.range`), `zipWith`, `collect`,
 *  `mapFilter` (cats syntax), `zipWithIndex`, `scan`, `drop`, `takeWhile`, `dropWhile`. */
private[fs2] final class Fs2Fuser(par: Option[Expr[Int]] = None)(using Quotes) extends ChainFuser:
  import q.reflect.*

  protected val streamClass = Symbol.requiredClass("fs2.Stream")
  private val StreamModule = Symbol.requiredModule("fs2.Stream")
  private val verbose = System.getProperty("strom.verbose") != null

  protected def elemOf(t: TypeRepr): TypeRepr = typeArgs(t)(1)
  private def effectOf(t: TypeRepr): TypeRepr = typeArgs(t)(0)

  protected def pureOp(c: Call): Option[PureOp] = (c.name, c.argss) match
    case ("map", List(List(f)))                => Some(PureOp.Map(f))
    case ("filter", List(List(p)))             => Some(PureOp.Filter(p))
    case ("take", List(List(n)))               => Some(PureOp.Take(n))
    case ("fold", List(List(z), List(f)))      => Some(PureOp.Fold(z, f))
    case ("collect", List(List(pf)))           => Some(PureOp.Collect(pf))
    case ("mapFilter*", List(List(f)))         => Some(PureOp.MapOption(f)) // cats FunctorFilter syntax
    case ("zipWithIndex", Nil)                 => Some(PureOp.ZipWithIndex())
    case ("zip", List(List(that)))             => Some(PureOp.Zip(that))
    case ("mapAccumulate", List(List(z), List(f))) => Some(PureOp.MapAccum(z, f, strom.internal.Pipe.AccumOut.PairOut))
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

  override protected def parallelism: Option[Term] = par.map(_.asTerm)

  /** `StromFs2.fusedParRun[F, B](plan, input, tag, n)(using Concurrent[F])` */
  override protected def emitParallel(r: Run, plan: Term, input: Term, tag: Term, n: Term): Option[Term] =
    val last = r.calls.last.term
    val (fRepr, bRepr) = (effectOf(last.tpe), elemOf(last.tpe))
    val concurrent = Symbol.requiredClass("cats.effect.kernel.GenConcurrent").typeRef.appliedTo(List(fRepr, TypeRepr.of[Throwable]))
    Implicits.search(concurrent) match
      case s: ImplicitSearchSuccess =>
        if verbose then report.info(s"strom: ${log.last}", last.pos)
        Some(Select.unique(Ref(Symbol.requiredModule("strom.fs2.StromFs2")), "fusedParRun").appliedToTypes(List(fRepr, bRepr))
          .appliedToArgs(List(plan, input, tag, n, Literal(IntConstant(Segment.DefaultMaxOut)), Literal(IntConstant(Segment.DefaultBudget))))
          .appliedTo(s.tree))
      case _: ImplicitSearchFailure =>
        log(log.size - 1) += ", sequential (no Concurrent)"
        None

  /** `StromFs2.fusedRun[F, B](factory, inputs, tags, cede, maxOut, budget)` */
  protected def emit(r: Run, sink: Pipe.Sink): Term =
    val last = r.calls.last.term
    val (fRepr, bRepr) = (effectOf(last.tpe), elemOf(last.tpe))
    val spawn = Symbol.requiredClass("cats.effect.kernel.GenSpawn").typeRef.appliedTo(List(fRepr, TypeRepr.of[Throwable]))
    val (cede, cedes) = Implicits.search(spawn) match
      case s: ImplicitSearchSuccess => (Select.unique(s.tree, "cede"), true)
      case _: ImplicitSearchFailure => ('{ null }.asTerm, false)
    if !cedes then log(log.size - 1) += ", never cedes"
    if verbose then report.info(s"strom: ${log.last}", last.pos)
    Select.unique(Ref(Symbol.requiredModule("strom.fs2.StromFs2")), "fusedRun").appliedToTypes(List(fRepr, bRepr))
      .appliedToArgs(runArgs(r, sink, bRepr) ++ List(cede, Literal(IntConstant(Segment.DefaultMaxOut)), Literal(IntConstant(Segment.DefaultBudget))))
