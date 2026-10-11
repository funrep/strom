package strom.internal

import scala.quoted.*
import scala.reflect.ClassTag

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
    /** A Mealy machine; `out` says how the step's result encodes the output. */
    case MapAccum(z: Term, f: Term, out: Pipe.AccumOut)
    /** `assoc`: the adapter knows `op` is associative with identity `z` (e.g. a cats `Monoid`). */
    case Fold(z: Term, op: Term, assoc: Boolean = false)
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
  /** A pure source the adapter recognises (`range`, `emit`, `emits`, `chunk`, …), as a pipe. */
  protected def source(t: Term): Option[Pipe[?]]
  /** Code running the compiled segment of `run`, typed like `run.calls.last.term`. */
  protected def emit(run: Run, sink: Pipe.Sink): Term

  /** In `fusedPar` mode: the requested parallelism. */
  protected def parallelism: Option[Term] = None
  /** Code running `plan` (a `ParallelPlan` of `run`'s single input `input`, with element class
   *  tag `tag`) on `n` workers, typed like `run.calls.last.term`; None if the stream's effect
   *  cannot run concurrently. */
  protected def emitParallel(run: Run, plan: Term, input: Term, tag: Term, n: Term): Option[Term] = None

  // ------------------------------------------------------------------ sources for adapters

  protected def rangeSource(from: Term, until: Term): Pipe[?] = Pipe.SrcRange(from.asExprOf[Int], until.asExprOf[Int])

  protected def oneSource(x: Term, elem: TypeRepr): Pipe[?] = elem.asType match
    case '[a] => Pipe.SrcOne[a](x.asExprOf[a])

  /** A `scala.collection.Seq` / `Iterable`: by index if it is an `IndexedSeq`, else by iterator. */
  protected def seqSource(xs: Term, elem: TypeRepr): Pipe[?] = elem.asType match
    case '[a] =>
      if xs.tpe <:< TypeRepr.of[collection.IndexedSeq[a]] then
        Pipe.SrcIndexed[a, collection.IndexedSeq[a]](xs.asExprOf[collection.IndexedSeq[a]],
          Pipe.Fn1(c => '{ $c.length }), Pipe.Fn2((c, i) => '{ $c($i) }))
      else Pipe.SrcIter[a]('{ ${ xs.asExprOf[Iterable[a]] }.iterator })

  // ------------------------------------------------------------------ machinery

  /** Type arguments of a stream type. */
  protected def typeArgs(t: TypeRepr): List[TypeRepr] = t.widen.dealias match
    case AppliedType(_, as) => as
    case other => report.errorAndAbort(s"strom: unexpected stream type ${other.show}")

  /** The leading arguments of an adapter's `fusedRun`: the segment factory for `sink` (with
   *  output type `out`), the input streams, and their element class tags. */
  protected def runArgs(r: Run, sink: Pipe.Sink, out: TypeRepr): List[Term] =
    val factory = out.asType match
      case '[b] => Codegen.segment[b](sink, r.inputs.toList.map(_._2.asType))
    val tags = r.inputs.map((_, t) => t.asType match
      case '[a] => Expr.summon[ClassTag[a]].getOrElse(report.errorAndAbort(s"strom: no ClassTag for ${t.show}")))
    List(factory.asTerm, '{ Vector[Any](${ Varargs(r.inputs.map(_._1.asExpr)) }*) }.asTerm,
      '{ Vector[ClassTag[?]](${ Varargs(tags) }*) }.asTerm)

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

  /** A flatMap body we can fuse: a pure source followed by pure map/filter/take/flatMap. */
  private def pureInner(t: Term): Boolean =
    val (src, calls) = chain(t)
    source(src).isDefined && calls.forall(c => op(c).exists {
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
    case _: PureOp.MapAccum => "mapAccum"
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
      val sink = r.fold.getOrElse(r.pipe match { case p: Pipe[a] => Pipe.EmitSink[a](p) })
      current =
        if r.worth || parallelisable(r, sink) then
          log += s"fused: ${r.names.mkString(" → ")} (${r.inputs.size} inputs)"
          parallel(r, sink).getOrElse(emit(r, sink))
        else r.calls.foldLeft(r.original)((acc, c) => rebuild(c.term, acc))
      run = None
    }

    def canAdd(o: PureOp): Boolean = o match
      case _: PureOp.ZipWith | _: PureOp.Zip => run.forall(_.inputs.size < strom.runtime.Segment.MaxPorts)
      case _ => true

    for c <- calls do
      op(c) match
        case Some(o) if canAdd(o) =>
          // fusedPar: a zip with another (effectful) stream gets a run of its own, so that the runs
          // before and after it have one input and can be planned in parallel.
          val ownRun = parallelism.isDefined && (o match
            case PureOp.Zip(that)        => !pureInner(that)
            case PureOp.ZipWith(that, _) => !pureInner(that)
            case _                       => false)
          if ownRun then flush()
          val r = run.getOrElse {
            val started = source(current) match
              case Some(src) => Run(current, src, Vector.empty)
              case None =>
                elemOf(current.tpe).asType match
                  case '[a] => Run(current, Pipe.SrcPort[a](0), Vector(current -> elemOf(current.tpe)))
            run = Some(started)
            started
          }
          add(r, c, o)
          if o.isInstanceOf[PureOp.Fold] || ownRun then flush()
        case _ =>
          kept += c.name.stripSuffix("*")
          flush()
          current = rebuild(c.term, current)
    flush()
    current

  // ------------------------------------------------------------------ fusedPar

  /** The parallel version of a run: only in `fusedPar` mode, for a run with one input whose
   *  plan has a parallel tier. Its lambdas must not capture mutable variables. */
  private def parallelisable(r: Run, sink: Pipe.Sink): Boolean =
    parallelism.isDefined && r.inputs.size == 1 && Parallel.describe(sink, r.inputs.head._2.asType).isDefined

  private def parallel(r: Run, sink: Pipe.Sink): Option[Term] =
    for
      n <- parallelism
      if r.inputs.size == 1
      port = r.inputs.head._2.asType
      desc <- Parallel.describe(sink, port)
      _ = r.calls.foreach(noCapturedVars)
      plan = ((Parallel.outType(sink): @unchecked) match { case '[o] => Parallel.plan[o](sink, List(port)).asTerm })
      tag = port match { case '[a] => Expr.summon[ClassTag[a]].getOrElse(report.errorAndAbort(s"strom: no ClassTag for ${r.inputs.head._2.show}")) }
      t <- emitParallel(r, plan, r.inputs.head._1, tag.asTerm, n)
    yield
      log(log.size - 1) += s", parallel: $desc"
      t

  /** Rejects a `var` read or written by an operator's arguments, unless defined inside them: run
   *  on several threads at once, it would be a data race. */
  private def noCapturedVars(c: Call): Unit =
    val local = collection.mutable.Set.empty[Symbol]
    val check = new TreeTraverser:
      override def traverseTree(t: Tree)(owner: Symbol): Unit =
        t match
          case d: Definition => local += d.symbol
          case r: Ref if r.symbol.flags.is(Flags.Mutable) && !local(r.symbol) =>
            report.errorAndAbort(s"strom: fusedPar runs `${c.name.stripSuffix("*")}` on several threads, but its argument uses " +
              s"the var `${r.symbol.name}` (a data race). Use `fused`, or avoid shared mutable state.", r.pos)
          case _ =>
        super.traverseTree(t)(owner)
    c.argss.flatten.foreach(a => check.traverseTree(a)(Symbol.spliceOwner))

  /** `(x, y) => x op y` with a literal identity `z` for an associative (and commutative)
   *  operator on Ints or Longs: `+`, `*`, `&`, `|`, `^`, `max`, `min` (`math.` or `Math.`). */
  private def associative(z: Term, f: Term): Boolean =
    val zero: Option[Long] = strip(z).tpe match
      case ConstantType(IntConstant(v))  => Some(v.toLong)
      case ConstantType(LongConstant(v)) => Some(v)
      case _ => None
    val tpeOk = z.tpe.widen =:= TypeRepr.of[Int] || z.tpe.widen =:= TypeRepr.of[Long]
    def isParams(a: Term, b: Term, ps: List[Symbol]) =
      (strip(a), strip(b)) match
        case (x: Ident, y: Ident) => Set(x.symbol, y.symbol) == ps.toSet && ps.size == 2
        case _ => false
    val op: Option[String] = strip(f) match
      case Lambda(List(p1, p2), body) =>
        val ps = List(p1.symbol, p2.symbol)
        strip(body) match
          case Apply(Select(a, name), List(b)) if isParams(a, b, ps) => Some(name)
          case Apply(fn, List(a, b)) if isParams(a, b, ps) && Set("max", "min")(fn.symbol.name) &&
              Set("scala.math.package$", "java.lang.Math")(fn.symbol.owner.fullName) => Some(fn.symbol.name)
          case _ => None
      case _ => None
    tpeOk && op.exists {
      case "+" | "|" | "^" => zero.contains(0L)
      case "*"             => zero.contains(1L)
      case "&"             => zero.contains(-1L)
      case "max" | "min"   => zero.isDefined
      case _               => false
    }

  private def add(r: Run, c: Call, o: PureOp): Unit =
    r.calls :+= c
    r.names :+= name(o)
    ((elemOf(c.qual.tpe).asType, elemOf(c.term.tpe).asType): @unchecked) match
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
          case PureOp.MapAccum(z, f, out) =>
            elemOf(c.qual.tpe).asType match
              case '[a0] =>
                z.tpe.widen.asType match
                  case '[st] =>
                    r.pipe = Pipe.MapAccumP[a, st, b](up, z.asExprOf[st],
                      front.fn2[st, a, Any](f.asExprOf[(st, a) => Any]), out)
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
          case PureOp.Fold(z, f, assoc) =>
            // A recognised associative operator over the element type allows parallel partial folds.
            val combine =
              if TypeRepr.of[a] =:= TypeRepr.of[b] && (assoc || associative(z, f)) then Some(front.fn2[b, b, b](f.asExprOf[(b, b) => b]))
              else None
            r.fold = Some(Pipe.FoldSink[a, b](up, z.asExprOf[b], front.fn2[b, a, b](f.asExprOf[(b, a) => b]),
              associative = combine.isDefined, combine))

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
    val r = Run(src, source(src).get, Vector.empty)
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
