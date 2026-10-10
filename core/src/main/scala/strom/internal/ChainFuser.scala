package strom.internal

import scala.quoted.*

/** Fusion of ordinary stream-library code (fs2, ZIO, …): decomposes a receiver chain
 *  `src.op1(..).op2(..)…`, groups maximal runs of pure operators into [[Pipe]]s and replaces each
 *  run by a compiled segment, leaving every other operator exactly as written.
 *
 *  Library-independent; adapters say which calls are pure operators and how to run a segment. */
abstract class ChainFuser(using val q: Quotes):
  import q.reflect.*

  // ------------------------------------------------------------------ adapter hooks

  /** Pure operators an adapter recognises, with the argument trees that matter. */
  enum PureOp:
    case Map(f: Term)
    case Filter(p: Term)
    case Take(n: Term)
    case FlatMap(f: Term)
    case ZipWith(that: Term, f: Term)
    case Fold(z: Term, op: Term)

  protected def streamClass: Symbol
  /** Element type of a stream type. */
  protected def elemOf(t: TypeRepr): TypeRepr
  /** The pure operator `c` is, if any (before checking flatMap bodies). */
  protected def pureOp(c: Call): Option[PureOp]
  /** `range(from, until)` over Ints, as a source. */
  protected def intRange(t: Term): Option[(Expr[Int], Expr[Int])]
  /** Maximum number of inputs a fused run may have (zipWith adds inputs). */
  protected def maxInputs: Int
  /** Whether a run may start from a range source with no input stream. */
  protected def sourcesSupported: Boolean
  /** Code running the compiled segment of `run`, typed like `run.calls.last.term`. */
  protected def emit(run: Run, sink: Pipe.Sink): Term

  // ------------------------------------------------------------------ machinery

  private val front = Frontend()
  /** One line per fused run. */
  val log = collection.mutable.ArrayBuffer.empty[String]

  protected def strip(t: Term): Term = t match
    case Inlined(_, Nil, e) => strip(e)
    case Typed(e, _)        => strip(e)
    case Block(Nil, e)      => strip(e)
    case _                  => t

  protected def isStream(t: TypeRepr): Boolean = t.widen.dealias.typeSymbol == streamClass

  /** `qual.name[targs](args1)(args2)…` */
  final case class Call(term: Term, qual: Term, name: String, targs: List[TypeRepr], argss: List[List[Term]])

  protected def asCall(t: Term): Option[Call] =
    def go(x: Term, targs: List[TypeRepr], argss: List[List[Term]]): Option[Call] = x match
      case Apply(fn, as)      => go(fn, targs, as :: argss)
      case TypeApply(fn, ts)  => go(fn, ts.map(_.tpe), argss)
      case Select(qual, name) => Some(Call(t, qual, name, targs, argss))
      case _                  => None
    go(t, Nil, Nil)

  /** The source of a receiver chain and the calls applied to it, innermost first. */
  protected def chain(t0: Term): (Term, List[Call]) =
    val t = strip(t0)
    asCall(t) match
      case Some(c) if isStream(c.qual.tpe) && isStream(t.tpe) =>
        val (src, calls) = chain(c.qual)
        (src, calls :+ c.copy(qual = strip(c.qual)))
      case _ => (t, Nil)

  protected def lambda1(t: Term): Option[(Symbol, Term)] = strip(t) match
    case Lambda(List(p), body) => Some((p.symbol, body))
    case _ => None

  /** A flatMap body we can fuse: a range followed by pure map/filter/take/flatMap. */
  private def pureInner(t: Term): Boolean =
    val (src, calls) = chain(t)
    intRange(src).isDefined && calls.forall(c => op(c).exists {
      case _: PureOp.ZipWith | _: PureOp.Fold => false
      case _ => true
    })

  private def op(c: Call): Option[PureOp] = pureOp(c).filter {
    case PureOp.FlatMap(f) => lambda1(f).exists((_, body) => pureInner(body))
    case _ => true
  }

  private def name(o: PureOp): String = o.productPrefix.updated(0, o.productPrefix.head.toLower)

  final class Run(val original: Term, var pipe: Pipe[?], var inputs: Vector[(Term, TypeRepr)]):
    var calls = Vector.empty[Call]
    var names = Vector.empty[String]
    var fold: Option[Pipe.Sink] = None
    def worth: Boolean =
      names.size >= 2 || inputs.isEmpty || names.exists(n => n == "flatMap" || n == "zipWith" || n == "fold")

  def fuse(e: Expr[Any]): Expr[Any] = fuse(e.asTerm).asExpr

  protected def fuse(t: Term): Term =
    val (src, calls) = chain(t)
    var current: Term = src
    var run: Option[Run] = None

    def flush(): Unit = run.foreach { r =>
      current =
        if r.worth then
          val sink = r.fold.getOrElse(r.pipe match { case p: Pipe[a] => Pipe.EmitSink[a](p) })
          log += s"fused: ${r.names.mkString(" → ")} (${r.inputs.size} inputs)"
          emit(r, sink)
        else r.calls.foldLeft(r.original)((acc, c) => rebuild(c.term, acc))
      run = None
    }

    def canAdd(o: PureOp): Boolean = o match
      case _: PureOp.ZipWith => run.forall(_.inputs.size < maxInputs)
      case _ => true

    for c <- calls do
      op(c) match
        case Some(o) if canAdd(o) && (run.isDefined || intRange(current).isEmpty || sourcesSupported) =>
          val r = run.getOrElse {
            val started = intRange(current).filter(_ => sourcesSupported) match
              case Some((from, until)) => Run(current, Pipe.SrcRange(from, until), Vector.empty)
              case None =>
                elemOf(current.tpe).asType match
                  case '[a] => Run(current, Pipe.SrcPort[a](0), Vector(current -> elemOf(current.tpe)))
            run = Some(started)
            started
          }
          add(r, c, o)
          if o.isInstanceOf[PureOp.Fold] then flush()
        case _ =>
          flush()
          current = rebuild(c.term, current)
    flush()
    current

  private def add(r: Run, c: Call, o: PureOp): Unit =
    r.calls :+= c
    r.names :+= name(o)
    (elemOf(c.qual.tpe).asType, elemOf(c.term.tpe).asType) match
      case ('[a], '[b]) =>
        val up = r.pipe.asInstanceOf[Pipe[a]]
        o match
          case PureOp.Map(f)    => r.pipe = Pipe.MapP[a, b](up, front.fn1[a, b](f.asExprOf[a => b]))
          case PureOp.Filter(p) => r.pipe = Pipe.FilterP[a](up, front.fn1[a, Boolean](p.asExprOf[a => Boolean]))
          case PureOp.Take(n)   => r.pipe = Pipe.TakeP[a](up, n.asExprOf[Long])
          case PureOp.FlatMap(f) =>
            val (param, body) = lambda1(f).get
            r.pipe = Pipe.FlatMapP[a, b](up, Pipe.Inner { x =>
              innerPipe(Frontend.subst(body, List(param), List(x))).asInstanceOf[Pipe[b]]
            })
          case PureOp.ZipWith(that, f) =>
            val port = r.inputs.size
            elemOf(that.tpe).asType match
              case '[t] =>
                r.inputs :+= (fuse(that) -> elemOf(that.tpe))
                r.pipe = Pipe.ZipWithP[a, t, b](up, Pipe.SrcPort[t](port), front.fn2[a, t, b](f.asExprOf[(a, t) => b]))
          case PureOp.Fold(z, f) =>
            r.fold = Some(Pipe.FoldSink[a, b](up, z.asExprOf[b], front.fn2[b, a, b](f.asExprOf[(b, a) => b]), associative = false))

  /** Pipe for a pure inner stream (checked by `pureInner`). */
  private def innerPipe(t: Any): Pipe[?] =
    val (src, calls) = chain(t.asInstanceOf[Term])
    val (from, until) = intRange(src).get
    val r = Run(src, Pipe.SrcRange(from, until), Vector.empty)
    calls.foreach(c => add(r, c, op(c).get))
    r.pipe

  /** The original call with a new receiver. */
  protected def rebuild(t: Term, newQual: Term): Term = t match
    case Apply(fn, as)     => Apply.copy(t)(rebuild(fn, newQual), as)
    case TypeApply(fn, ts) => TypeApply.copy(t)(rebuild(fn, newQual), ts)
    case Select(_, name)   => Select.copy(t)(newQual, name)
    case _                 => report.errorAndAbort(s"strom: cannot rebuild ${t.show}")
