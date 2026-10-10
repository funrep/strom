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
    /** `zip`: pairs, built from staging-time components (unboxed unless they escape). */
    case Zip(that: Term)
    case Fold(z: Term, op: Term)
    case Collect(pf: Term)
    case MapOption(f: Term)
    case ZipWithIndex()
    case Scan(z: Term, op: Term)
    /** `n` must be a `Long` term. */
    case Drop(n: Term)
    case TakeWhile(p: Term)
    case DropWhile(p: Term)

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
  /** Operators that were kept as written (not fusable), in order of appearance. */
  val kept = collection.mutable.ArrayBuffer.empty[String]

  /** `log`, plus a `kept:` line listing the operators left unfused (for `fusedPlan`). */
  def summary: String = (log ++ (if kept.isEmpty then Nil else List(s"kept: ${kept.mkString(", ")}"))).mkString("\n")

  protected def strip(t: Term): Term = t match
    case Inlined(_, Nil, e) => strip(e)
    case Typed(e, _)        => strip(e)
    case Block(Nil, e)      => strip(e)
    // Default/named arguments: `{ val qual$1 = s; val x$2 = qual$1.op$default$2; qual$1.op(p, x$2) }`.
    // Stream values are pure descriptions, so the synthetic vals can be substituted back.
    case Block(stats, e) if stats.forall(isSyntheticVal) =>
      val vals = stats.collect { case v: ValDef => v }
      strip(vals.foldRight(e)((v, acc) => Frontend.subst(acc, List(v.symbol), List(v.rhs.get.asExpr)).asInstanceOf[Term]))
    case _                  => t

  private def isSyntheticVal(s: Statement): Boolean = s match
    case v: ValDef => v.rhs.isDefined && !v.symbol.flags.is(Flags.Mutable) &&
      (v.symbol.flags.is(Flags.Synthetic) || v.name.contains("$"))
    case _ => false

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

  /** The source of a receiver chain and the calls applied to it, innermost first. Besides
   *  `stream.op(..)`, receivers reached through an implicit conversion (`ops(stream).op(..)`,
   *  e.g. cats syntax) or an extension method (`op(stream)(..)`) are followed too; such calls
   *  are always kept as written. */
  protected def chain(t0: Term): (Term, List[Call]) =
    val t = strip(t0)
    if !isStream(t.tpe) then (t, Nil)
    else asCall(t) match
      case Some(c) if isStream(c.qual.tpe) =>
        val (src, calls) = chain(c.qual)
        (src, calls :+ c.copy(qual = strip(c.qual)))
      case Some(c) =>
        indirectReceiver(t) match
          case Some((recv, _)) =>
            val (src, calls) = chain(recv)
            (src, calls :+ c.copy(qual = strip(recv), name = c.name + "*"))
          case None => (t, Nil)
      case None => (t, Nil)

  /** For `conv(stream).op(..)` or `op(stream)(..)`: the stream, and how to rebuild with another. */
  private def indirectReceiver(t: Term): Option[(Term, Term => Term)] =
    def viaConversion(x: Term): Option[(Term, Term => Term)] = x match
      case Apply(fn, as)     => viaConversion(fn).map((r, rb) => (r, (n: Term) => Apply.copy(x)(rb(n), as)))
      case TypeApply(fn, ts) => viaConversion(fn).map((r, rb) => (r, (n: Term) => TypeApply.copy(x)(rb(n), ts)))
      case Select(q, name) => conversionArg(strip(q)).map((r, rb) => (r, (n: Term) => Select.copy(x)(rb(n), name)))
      case _ => None
    // conv(stream)(using instances…)
    def conversionArg(c: Term): Option[(Term, Term => Term)] = c match
      case Apply(f, List(arg)) if isStream(arg.tpe) => Some((arg, (n: Term) => Apply.copy(c)(f, List(n))))
      case Apply(f, as)     => conversionArg(f).map((r, rb) => (r, (n: Term) => Apply.copy(c)(rb(n), as)))
      case TypeApply(f, ts) => conversionArg(f).map((r, rb) => (r, (n: Term) => TypeApply.copy(c)(rb(n), ts)))
      case _ => None
    def viaExtension(x: Term): Option[(Term, Term => Term)] = x match
      case Apply(fn, as) =>
        viaExtension(fn).map((r, rb) => (r, (n: Term) => Apply.copy(x)(rb(n), as))).orElse {
          // the innermost argument list holding a single stream: the extension receiver
          as match
            case List(arg) if isStream(arg.tpe) && innermost(fn) => Some((arg, (n: Term) => Apply.copy(x)(fn, List(n))))
            case _ => None
        }
      case TypeApply(fn, ts) => viaExtension(fn).map((r, rb) => (r, (n: Term) => TypeApply.copy(x)(rb(n), ts)))
      case _ => None
    def innermost(fn: Term): Boolean = fn match
      case TypeApply(f, _) => innermost(f)
      case _: Ident | _: Select => true
      case _ => false
    viaConversion(t).orElse(viaExtension(t))

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

  private def name(o: PureOp): String = o match
    case _: PureOp.MapOption => "mapFilter"
    case _: PureOp.Zip => "zip"
    case _ => o.productPrefix.updated(0, o.productPrefix.head.toLower)

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
      case _: PureOp.ZipWith | _: PureOp.Zip => run.forall(_.inputs.size < maxInputs)
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
          kept += c.name.stripSuffix("*")
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
          case PureOp.Zip(that) =>
            elemOf(that.tpe).asType match
              case '[t] =>
                val right = zipOperand[t](r, that)
                r.pipe = Pipe.ZipWithP[a, t, b](up, right, Pipe.Fn2((x, y) => '{ ($x, $y) }.asExprOf[b]))
          case PureOp.ZipWith(that, f) =>
            elemOf(that.tpe).asType match
              case '[t] =>
                // A pure operand (a range plus pure operators) joins this segment; anything else
                // is fused on its own and becomes another input.
                val right =
                  if pureInner(that) then innerPipe(that).asInstanceOf[Pipe[t]]
                  else
                    val port = r.inputs.size
                    r.inputs :+= (fuse(that) -> elemOf(that.tpe))
                    Pipe.SrcPort[t](port)
                r.pipe = Pipe.ZipWithP[a, t, b](up, right, front.fn2[a, t, b](f.asExprOf[(a, t) => b]))
          case PureOp.Collect(pf) => r.pipe = Pipe.CollectP[a, b](up, front.partial[a, b](pf.asExprOf[PartialFunction[a, b]]))
          case PureOp.MapOption(f) =>
            r.pipe = Pipe.MapOptionP[a, b](up, front.fn1[a, Option[b]](f.asExprOf[a => Option[b]]))
          case PureOp.ZipWithIndex() =>
            r.pipe = Pipe.ZipWithIndexP[a](up)(using Type.of[b].asInstanceOf[Type[(a, Long)]])
          case PureOp.Scan(z, f) => r.pipe = Pipe.ScanP[a, b](up, z.asExprOf[b], front.fn2[b, a, b](f.asExprOf[(b, a) => b]))
          case PureOp.Drop(n) => r.pipe = Pipe.DropP[a](up, n.asExprOf[Long])
          case PureOp.TakeWhile(p) => r.pipe = Pipe.TakeWhileP[a](up, front.fn1[a, Boolean](p.asExprOf[a => Boolean]))
          case PureOp.DropWhile(p) => r.pipe = Pipe.DropWhileP[a](up, front.fn1[a, Boolean](p.asExprOf[a => Boolean]))
          case PureOp.Fold(z, f) =>
            r.fold = Some(Pipe.FoldSink[a, b](up, z.asExprOf[b], front.fn2[b, a, b](f.asExprOf[(b, a) => b]), associative = false))

  /** A zip operand: inlined if pure, else fused on its own and added as an input. */
  private def zipOperand[t: Type](r: Run, that: Term): Pipe[t] =
    if pureInner(that) then innerPipe(that).asInstanceOf[Pipe[t]]
    else
      val port = r.inputs.size
      r.inputs :+= (fuse(that) -> elemOf(that.tpe))
      Pipe.SrcPort[t](port)

  /** Pipe for a pure inner stream (checked by `pureInner`). */
  private def innerPipe(t: Any): Pipe[?] =
    val (src, calls) = chain(t.asInstanceOf[Term])
    val (from, until) = intRange(src).get
    val r = Run(src, Pipe.SrcRange(from, until), Vector.empty)
    calls.foreach(c => add(r, c, op(c).get))
    r.pipe

  /** The original call with a new receiver. */
  protected def rebuild(t: Term, newQual: Term): Term =
    asCall(t) match
      case Some(c) if !isStream(c.qual.tpe) =>
        indirectReceiver(t).map((_, rb) => rb(newQual)).getOrElse(report.errorAndAbort(s"strom: cannot rebuild ${t.show}"))
      case _ => rebuildDirect(t, newQual)

  private def rebuildDirect(t: Term, newQual: Term): Term = t match
    case Apply(fn, as)     => Apply.copy(t)(rebuildDirect(fn, newQual), as)
    case TypeApply(fn, ts) => TypeApply.copy(t)(rebuildDirect(fn, newQual), ts)
    case Select(_, name)   => Select.copy(t)(newQual, name)
    case _                 => report.errorAndAbort(s"strom: cannot rebuild ${t.show}")
