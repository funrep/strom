package strom.internal

import scala.quoted.*
import scala.collection.mutable
import scala.util.chaining.*
import strom.runtime.Segment.Status

/** The staged code generator: Kiselyov's stream combinators over [[StStream]], plus two
 *  backends that turn a stream into loops.
 *
 *   - `nest`  — Kiselyov's `fold_raw`: properly nested loops, the fastest shape, used for
 *               direct (whole-input) pipelines.
 *   - `flags` — `make_linear` generalised: every nesting level keeps an `active` flag and all
 *               state lives in [[Storage]], so the loop nest can be left at any element
 *               boundary and re-entered later. Used for zip/window operands (pulls) and for
 *               every loop of a resumable segment.
 *
 *  In resumable mode, the generated code may `return` a [[Status]] from the step method
 *  (input exhausted, output full, budget exhausted, done). Suspension only ever happens
 *  *before* an element is consumed, or after it has been stored in state, so re-entering
 *  the loops from the top continues exactly where they stopped.
 */
final class Gen(using val q: Quotes)(
    val storage: Storage,
    val mode: Gen.Mode,
    val split: Gen.SplitPlan,
    rootOwner: q.reflect.Symbol,
    describing: Boolean = false,
):
  import q.reflect.*
  import Gen.*

  // ------------------------------------------------------------------ small code helpers

  val unit: Expr[Unit] = '{ () }

  private def isUnit(e: Expr[?]): Boolean = e.asTerm match
    case Literal(UnitConstant()) => true
    case Inlined(_, Nil, Literal(UnitConstant())) => true
    case _ => false

  /** Sequences statements, dropping `()` and flattening blocks, so `Expr.show` stays readable. */
  def seq(es: Expr[Unit]*): Expr[Unit] =
    val stats = es.toList.filterNot(isUnit).flatMap { e =>
      e.asTerm match
        case Block(ss, Literal(UnitConstant())) => ss
        case t => List(t)
    }
    stats match
      case Nil       => unit
      case List(one) => one.asExprOf[Unit]
      case _ =>
        stats.last match
          case last: Term => Block(stats.init, last).asExprOf[Unit]
          case _          => Block(stats, Literal(UnitConstant())).asExprOf[Unit]

  private def trivial(t: Term): Boolean = t match
    case Literal(_) => true
    case Ident(_)   => !t.symbol.flags.is(Flags.Mutable)
    case Inlined(_, Nil, e) => trivial(e)
    case _ => false

  /** Let-insertion: bind a computed element to a `val` before it can be duplicated downstream. */
  def letIn[A: Type](e: Expr[A])(k: Expr[A] => Expr[Unit]): Expr[Unit] =
    val t = e.asTerm
    Tuples.components(t) match
      // A tuple is not allocated: its components are bound, and the constructor over the bound
      // variables is passed on, to be simplified away by projections / tuple patterns downstream.
      case Some(args) if !args.forall(trivial) =>
        def go(rest: List[Term], acc: List[Term]): Expr[Unit] = rest match
          case Nil => k(rebuildTuple(t, acc.reverse).asExprOf[A])
          case a :: tl if trivial(a) => go(tl, a :: acc)
          case a :: tl => a.tpe.widen.asType match
            case '[c] => bind("c", a.asExprOf[c])(v => go(tl, v.asTerm :: acc))
        go(args, Nil)
      case Some(_) => k(e)
      case None => if trivial(t) then k(e) else bind("y", e)(k)

  private def rebuildTuple(t: Term, args: List[Term]): Term = t match
    case Inlined(_, Nil, inner) => rebuildTuple(inner, args)
    case Typed(inner, _)        => rebuildTuple(inner, args)
    case a @ Apply(fn, _)       => Apply.copy(a)(fn, args)
    case _ => report.errorAndAbort("internal: not a tuple constructor")

  /** `{ val name = rhs; body(name) }`. Binders are built with reflection rather than as
   *  `'{ val y = ..; ${ k('y) } }`, so that no code value escapes the splice it was made in. */
  def bind[T: Type](name: String, rhs: Expr[T])(body: Expr[T] => Expr[Unit]): Expr[Unit] =
    val sym = Symbol.newVal(curOwner, name, TypeRepr.of[T].widen, Flags.EmptyFlags, Symbol.noSymbol)
    val b = body(Ref(sym).asExprOf[T])
    Block(List(ValDef(sym, Some(rhs.asTerm.changeOwner(sym)))), b.asTerm).asExprOf[Unit]

  def alloc[T: Type](hint: String): Var[T] = storage.alloc[T](hint)

  def inc(v: Var[Int], by: Int = 1): Expr[Unit] = v := '{ ${ v.get } + ${ Expr(by) } }

  /** The method currently being generated into (the step method, or an outlined part). */
  private var curOwner: Symbol = rootOwner

  private def self: Term = mode match
    case m: Mode.Resumable => m.self.asInstanceOf[Term]
    case Mode.Direct       => report.errorAndAbort("internal: no segment instance in direct mode")

  def fld[T: Type](name: String): Expr[T] = Select.unique(self, name).asExprOf[T]
  def setFld[T](name: String, v: Expr[T]): Expr[Unit] = Assign(Select.unique(self, name), v.asTerm).asExprOf[Unit]

  /** `return code` from the current method (resumable mode only). */
  def ret(code: Expr[Int]): Expr[Unit] = mode match
    case Mode.Direct => report.errorAndAbort("internal: suspension in direct mode")
    case _           => Return(code.asTerm, curOwner).asExprOf[Unit]

  private def resumable = mode != Mode.Direct

  /** `outer`: charged before stepping an outer level; `inner`: before stepping the innermost
   *  level (fuel, plus the output-full check when feeding the emit sink). */
  final case class Ticks(outer: () => Expr[Unit], inner: () => Expr[Unit], full: () => Expr[Unit] = () => '{ () })

  private def fullCheck(t: Ticks): Expr[Unit] = t.full()

  private def fuelTick(): Expr[Unit] =
    if !resumable then unit
    else '{ if (${ fld[Int]("fuel") } <= 0) ${ ret(Expr(Status.Yield)) }; ${ setFld("fuel", '{ ${ fld[Int]("fuel") } - 1 }) } }

  /** Ticks for loops that run inside a pull (zip / window operands). */
  val pullTicks: Ticks = Ticks(() => fuelTick(), () => fuelTick())

  // ------------------------------------------------------------------ method splitting

  private var nextCandidate = 0
  private val parents = mutable.Stack[Int]()

  /** Marks `body` as a code region that may be outlined into a local method (lifted by the
   *  compiler to a separate JVM method) to keep generated methods under HotSpot's 8000-byte
   *  compilation limit. In resumable mode an outlined part returns a status. */
  def candidate(body: => Expr[Unit]): Expr[Unit] =
    val id = nextCandidate
    nextCandidate += 1
    split.parent(id) = parents.headOption.getOrElse(-1)
    parents.push(id)
    try
      if !split.outline(id) then
        val b = body
        split.sizes(id) = treeSize(b)
        b
      else
        val resTpe = if resumable then TypeRepr.of[Int] else TypeRepr.of[Unit]
        val sym = Symbol.newMethod(curOwner, s"part$id", MethodType(Nil)(_ => Nil, _ => resTpe))
        val outer = curOwner
        curOwner = sym
        val b = try body finally curOwner = outer
        val rhs0 = if resumable then Block(List(b.asTerm), Literal(IntConstant(Status.Continue))) else b.asTerm
        val rhs = retarget(rhs0, sym)
        val ddef = DefDef(sym, _ => Some(rhs.changeOwner(sym)))
        val call = Apply(Ref(sym), Nil)
        if !resumable then Block(List(ddef), call).asExprOf[Unit]
        else
          val st = Symbol.newVal(curOwner, s"st$id", TypeRepr.of[Int], Flags.EmptyFlags, Symbol.noSymbol)
          val stRef = Ref(st).asExprOf[Int]
          val check = '{ if ($stRef != ${ Expr(Status.Continue) }) ${ ret(stRef) } }
          Block(List(ddef, ValDef(st, Some(call))), check.asTerm).asExprOf[Unit]
    finally parents.pop()

  /** Code may have been generated before the decision to outline it, so its `return`s can
   *  still target an enclosing method; make them return from the outlined part instead (the
   *  call site propagates the status). Returns from parts nested inside `t` are left alone. */
  private def retarget(t: Term, part: Symbol): Term =
    val nested = mutable.Set.empty[Symbol]
    new TreeTraverser:
      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        tree match
          case d: DefDef => nested += d.symbol
          case _ => ()
        super.traverseTree(tree)(owner)
    .traverseTree(t)(part)
    val mapper = new TreeMap:
      override def transformTerm(tree: Term)(owner: Symbol): Term = tree match
        case r @ Return(expr, from) if !nested(from) => Return.copy(r)(transformTerm(expr)(owner), part)
        case _ => super.transformTerm(tree)(owner)
    mapper.transformTerm(t)(part)

  // ------------------------------------------------------------------ Kiselyov combinators

  private def labeled[P <: Producer[?]](p: P, label: String): P = { p.label = label; p }
  private def derived[P <: Producer[?]](p: P, from: Producer[?], suffix: String = ""): P =
    p.label = from.label + suffix
    p.composite = from.composite
    p

  def forUnfold[A](p: Producer[A]): UnfoldP[A, ?] = p match
    case u: UnfoldP[A, s] => u
    case f: ForP[A, s] =>
      given Type[A] = f.tpe
      val i = alloc[Int]("i")
      derived(UnfoldP[A, s](
        init = k => f.init(s => seq(i := '{ 0 }, k(s))),
        term = s => '{ ${ i.get } < ${ f.upb(s) } },
        card = Card.Many,
        step = s => k => bind("idx", i.get)(idx => seq(i := '{ $idx + 1 }, f.index(s, idx)(k))),
        suspends = false,
      ), f)

  def card(p: Producer[?]): Card = p match
    case _: ForP[?, ?]    => Card.Many
    case u: UnfoldP[?, ?] => u.card

  def suspends(s: StStream[?]): Boolean = s match
    case Linear(p)       => p.suspends
    case n: Nested[?, ?] => n.p.suspends

  def withInit[A](p: Producer[A], pre: () => Expr[Unit]): Producer[A] = p match
    case f: ForP[A, s]    => derived(ForP[A, s](k => seq(pre(), f.init(k)), f.upb, f.index)(using f.tpe), f)
    case u: UnfoldP[A, s] => derived(UnfoldP[A, s](k => seq(pre(), u.init(k)), u.term, u.card, u.step, u.suspends)(using u.tpe), u)

  def addTerm[A](p: Producer[A], cond: () => Expr[Boolean]): Producer[A] = forUnfold(p) match
    case u: UnfoldP[A, s] =>
      derived(UnfoldP[A, s](u.init, s => '{ ${ cond() } && ${ u.term(s) } }, u.card, u.step, u.suspends)(using u.tpe), u)

  /** Pushes an extra termination condition into every loop of a stream. */
  def moreTermination[A](s: StStream[A], cond: () => Expr[Boolean]): StStream[A] = s match
    case Linear(p)       => Linear(addTerm(p, cond))
    case n: Nested[A, b] => Nested[A, b](addTerm(n.p, cond), x => moreTermination(n.f(x), cond))(using n.tpe)

  def mapProd[A, B: Type](p: Producer[A], tr: Expr[A] => (Expr[B] => Expr[Unit]) => Expr[Unit]): Producer[B] = p match
    case f: ForP[A, s]    => derived(ForP[B, s](f.init, f.upb, (s, i) => k => f.index(s, i)(a => tr(a)(k))), f)
    case u: UnfoldP[A, s] => derived(UnfoldP[B, s](u.init, u.term, u.card, s => k => u.step(s)(a => tr(a)(k)), u.suspends), u)

  def mapRaw[A, B: Type](s: StStream[A], tr: Expr[A] => (Expr[B] => Expr[Unit]) => Expr[Unit]): StStream[B] = s match
    case Linear(p)       => Linear(mapProd(p, tr))
    case n: Nested[A, b] => Nested[B, b](n.p, x => mapRaw(n.f(x), tr))

  def map[A, B: Type](s: StStream[A], f: Expr[A] => Expr[B]): StStream[B] =
    mapRaw[A, B](s, a => k => letIn(f(a))(k))

  def filter[A](s: StStream[A], pred: Expr[A] => Expr[Boolean]): StStream[A] = s match
    case Linear(p) =>
      forUnfold(p) match
        case u: UnfoldP[A, s] =>
          Linear(derived(UnfoldP[A, s](u.init, u.term, Card.AtMost1,
            st => k => u.step(st)(a => '{ if (${ pred(a) }) ${ k(a) } }), u.suspends)(using u.tpe), u, "|filter"))
    case n: Nested[A, b] => Nested[A, b](n.p, x => filter(n.f(x), pred))(using n.tpe)

  /** A partial map: `tr(a, k)` calls `k` at most once, so every producer becomes `AtMost1`. */
  def mapPartial[A, B: Type](s: StStream[A], tr: (Expr[A], Expr[B] => Expr[Unit]) => Expr[Unit], label: String): StStream[B] =
    s match
      case Linear(p) =>
        forUnfold(p) match
          case u: UnfoldP[A, st] =>
            Linear(derived(UnfoldP[B, st](u.init, u.term, Card.AtMost1, st => k => u.step(st)(a => tr(a, k)), u.suspends), u, "|" + label))
      case n: Nested[A, b] => Nested[B, b](n.p, x => mapPartial(n.f(x), tr, label))

  /** Runs `pre` when the stream (its outermost producer) starts. */
  private def withOuterInit[A](s: StStream[A], pre: () => Expr[Unit]): StStream[A] = s match
    case Linear(p)       => Linear(withInit(p, pre))
    case n: Nested[A, b] => Nested[A, b](withInit(n.p, pre), n.f)(using n.tpe)

  def mapOption[A, B: Type](s: StStream[A], f: Expr[A] => Expr[Option[B]]): StStream[B] =
    mapPartial[A, B](s, (a, k) => bind("o", f(a)) { o =>
      val body = k('{ $o.get })
      '{ if ($o.isDefined) $body }
    }, "mapFilter")

  def zipWithIndex[A](s: StStream[A]): StStream[(A, Long)] =
    given Type[A] = s.tpe
    val idx = alloc[Long]("idx")
    mapRaw[A, (A, Long)](withOuterInit(s, () => idx := '{ 0L }),
      a => k => bind("i", idx.get)(i => seq(idx := '{ $i + 1L }, letIn('{ ($a, $i) })(k))))

  def drop[A](s: StStream[A], n: Expr[Long]): StStream[A] =
    given Type[A] = s.tpe
    val rem = alloc[Long]("drop")
    mapPartial[A, A](withOuterInit(s, () => rem := n), (a, k) => {
      val body = k(a)
      val dec = rem := '{ ${ rem.get } - 1L }
      '{ if (${ rem.get } > 0L) $dec else $body }
    }, "drop")

  /** Stops every loop at the first element failing `p` (like `take`, pushed into all loops). */
  def takeWhile[A](s: StStream[A], p: Expr[A] => Expr[Boolean]): StStream[A] =
    given Type[A] = s.tpe
    val going = alloc[Boolean]("going")
    val s1 = withOuterInit(moreTermination(s, () => going.get), () => going := '{ true })
    mapPartial[A, A](s1, (a, k) => {
      val body = k(a)
      val stop = going := '{ false }
      '{ if (${ p(a) }) $body else $stop }
    }, "takeWhile")

  def dropWhile[A](s: StStream[A], p: Expr[A] => Expr[Boolean]): StStream[A] =
    given Type[A] = s.tpe
    val dropping = alloc[Boolean]("dropping")
    mapPartial[A, A](withOuterInit(s, () => dropping := '{ true }), (a, k) => {
      val body = seq(dropping := '{ false }, k(a))
      '{ if (${ dropping.get } && ${ p(a) }) () else $body }
    }, "dropWhile")

  /** A Mealy machine. The step's result `(s', out)` is taken apart at staging time: through
   *  blocks, `if`s and tuple / `Some` / `None` constructors, so the common case allocates neither a
   *  tuple nor an `Option`; anything else is bound and projected. The output is staged into a
   *  variable so that the downstream continuation is spliced once. */
  def mapAccum[A, S: Type, B: Type](s: StStream[A], z: Expr[S], f: (Expr[S], Expr[A]) => Expr[Any], mode: Pipe.AccumOut): StStream[B] =
    given Type[A] = s.tpe
    val acc = alloc[S]("state")
    val outV = alloc[B]("out")
    val have = alloc[Boolean]("emit")
    def setOut(b: Expr[B]): Expr[Unit] = seq(outV := b, have := '{ true })
    def option(o: Term): Expr[Unit] = strip(o) match
      case Block(stats, e) => Block(stats, option(e).asTerm).asExprOf[Unit]
      case If(c, t, e)     => If(c, option(t).asTerm, option(e).asTerm).asExprOf[Unit]
      case SomeOf(x)       => setOut(x.asExprOf[B])
      case t if t.symbol == NoneSym => unit
      case t => bind("o", t.asExprOf[Option[B]])(ov => { val set = setOut('{ $ov.get }); '{ if ($ov.isDefined) $set } })
    def output(ns: Expr[S], o: Term): Expr[Unit] = mode match
      case Pipe.AccumOut.OptionOut => option(o)
      case Pipe.AccumOut.AlwaysOut => setOut(o.asExprOf[B])
      case Pipe.AccumOut.PairOut   =>
        o.tpe.widen.asType match
          case '[t] => letIn(o.asExprOf[t])(b => setOut(Tuples.tupleOf(List(ns.asTerm, b.asTerm)).asExprOf[B]))
    def branch(r: Term): Expr[Unit] = strip(r) match
      case Block(stats, e) => Block(stats, branch(e).asTerm).asExprOf[Unit]
      case If(c, t, e)     => If(c, branch(t).asTerm, branch(e).asTerm).asExprOf[Unit]
      case t => Tuples.components(t) match
        case Some(List(st, o)) => bind("ns", st.asExprOf[S])(ns => seq(output(ns, o), acc := ns))
        case _ =>
          bind("r", t.asExpr) { rv =>
            bind("ns", Select.unique(rv.asTerm, "_1").asExprOf[S])(ns => seq(output(ns, Select.unique(rv.asTerm, "_2")), acc := ns))
          }
    mapPartial[A, B](withOuterInit(s, () => acc := z), (a, k) => {
      val emit = seq(have := '{ false }, k(outV.get))
      seq(branch(f(acc.get, a).asTerm), '{ if (${ have.get }) $emit })
    }, "mapAccum")

  private def strip(t: Term): Term = t match
    case Inlined(_, Nil, e) => strip(e)
    case Typed(e, _)        => strip(e)
    case _                  => t
  private val NoneSym = Symbol.requiredModule("scala.None")
  private object SomeOf:
    def unapply(t: Term): Option[Term] = t match
      case Apply(TypeApply(Select(m, "apply"), _), List(x)) if m.symbol == Symbol.requiredModule("scala.Some") => Some(x)
      case Apply(Select(m, "apply"), List(x)) if m.symbol == Symbol.requiredModule("scala.Some") => Some(x)
      case _ => None

  /** Emits `z`, then the running fold. A pull over the upstream; the continuation is spliced once. */
  def scan[A, S: Type](s: StStream[A], z: Expr[S], op: (Expr[S], Expr[A]) => Expr[S]): StStream[S] =
    val acc = alloc[S]("scan")
    val started = alloc[Boolean]("started")
    val emit = alloc[Boolean]("emit")
    val pl = pull(s)
    Linear(labeled(UnfoldP[S, Unit](
      k => seq(acc := z, started := '{ false }, emit := '{ false }, pl.init, k(())),
      _ => '{ !${ started.get } || !${ pl.ended.get } },
      Card.AtMost1,
      _ => k =>
        val first = seq(started := '{ true }, emit := '{ true })
        val next = seq(pl.have := '{ false }, acc := op(acc.get, pl.cur.get), emit := '{ true })
        val out = seq(emit := '{ false }, bind("s", acc.get)(k))
        seq('{ if (!${ started.get }) $first else { ${ pl.fill }; if (${ pl.have.get }) $next } }, '{ if (${ emit.get }) $out }),
      suspends(s)), "scan").tap(_.composite = true))

  def flatMap[A, B: Type](s: StStream[A], f: Expr[A] => StStream[B]): StStream[B] = s match
    case Linear(p)       => Nested[B, A](p, f)
    case n: Nested[A, c] => Nested[B, c](n.p, x => flatMap(n.f(x), f))

  /** `take(n)` with a `Long` count (fs2 counts in `Long`). Pushed into every loop. */
  def take[A](s: StStream[A], n: Expr[Long]): StStream[A] =
    given Type[A] = s.tpe
    def dec(v: Var[Long]): Expr[Unit] = v := '{ ${ v.get } - 1L }
    s match
      case Linear(f: ForP[A, s]) =>
        val nv = alloc[Long]("n")
        Linear(derived(ForP[A, s](k => seq(nv := n, f.init(k)),
          st => '{ Math.min(${ f.upb(st) }.toLong, ${ nv.get }).toInt }, f.index), f, "|take"))
      case Linear(u: UnfoldP[A, s]) =>
        val nr = alloc[Long]("nr")
        Linear(derived(UnfoldP[A, s](k => seq(nr := n, u.init(k)), st => '{ ${ nr.get } > 0L && ${ u.term(st) } }, u.card,
          st => k => u.step(st)(a => seq(dec(nr), k(a))), u.suspends), u, "|take"))
      case nst: Nested[A, b] =>
        val nr = alloc[Long]("nr")
        val more = () => '{ ${ nr.get } > 0L }
        val outer = withInit(addTerm(nst.p, more), () => nr := n)
        derived(outer, nst.p, "|take(all levels)")
        Nested[A, b](outer, x => mapRaw[A, A](moreTermination(nst.f(x), more), a => k => seq(dec(nr), k(a))))

  /** A stream linearised into a pull: `fill` runs the stream's loops until an element is
   *  stored in `cur` (`have`), or the stream is exhausted (`ended`). */
  final class Pull[A](val init: Expr[Unit], val fill: Expr[Unit], val have: Var[Boolean], val ended: Var[Boolean], val cur: Var[A])

  def pull[A](s: StStream[A]): Pull[A] =
    given Type[A] = s.tpe
    val have = alloc[Boolean]("have")
    val ended = alloc[Boolean]("ended")
    val cur = alloc[A]("cur")
    val prep = flags(s, List(() => '{ !${ have.get } && !${ ended.get } }), a => seq(cur := a, have := '{ true }),
      ended := '{ true }, pullTicks)
    Pull(seq(have := '{ false }, ended := '{ false }, prep.init), candidate(prep.loop), have, ended, cur)

  def zipWith[A, B, C: Type](l: StStream[A], r: StStream[B], f: (Expr[A], Expr[B]) => Expr[C]): StStream[C] =
    // Shapes are computed only for `Strom.explain`: describing a nested stream instantiates its inner stream.
    val (ls, rs) = if describing then (StStream.shape(l), StStream.shape(r)) else ("", "")
    def lab(kind: String)(s: StStream[C]): StStream[C] = s match
      case Linear(p) =>
        // Only the label: whether a producer is composite comes from the producers it was built from.
        p.label = s"$kind($ls, $rs)"
        s
      case _ => s
    (l, r) match
      // Two indexed producers: one shared index.
      case (Linear(lp: ForP[A, s1]), Linear(rp: ForP[B, s2])) => lab("zip:index")(
        Linear(ForP[C, (s1, s2)](
          k => lp.init(a => rp.init(b => k((a, b)))),
          s => '{ Math.min(${ lp.upb(s._1) }, ${ rp.upb(s._2) }) },
          (s, i) => k => lp.index(s._1, i)(a => rp.index(s._2, i)(b => letIn(f(a, b))(k))))))
      // Two exactly-one producers, at most one of which can suspend: step both in lock-step,
      // stepping the one that can suspend first so that no element is ever consumed and lost.
      case (Linear(lp), Linear(rp)) if card(lp) == Card.Many && card(rp) == Card.Many && !(lp.suspends && rp.suspends) =>
        (forUnfold(lp), forUnfold(rp)) match
          case (u1: UnfoldP[A, s1], u2: UnfoldP[B, s2]) => lab("zip:lockstep")(
            Linear(UnfoldP[C, (s1, s2)](
              k => u1.init(a => u2.init(b => k((a, b)))),
              s => '{ ${ u1.term(s._1) } && ${ u2.term(s._2) } },
              Card.Many,
              s => k =>
                if !u2.suspends then u1.step(s._1)(a => u2.step(s._2)(b => letIn(f(a, b))(k)))
                else u2.step(s._2)(b => u1.step(s._1)(a => letIn(f(a, b))(k))),
              u1.suspends || u2.suspends)))
      // One side yields exactly one element per step and cannot suspend: the other side drives in
      // its own (possibly nested) loops and this side is stepped inside (strymonas' push_linear).
      case (Linear(lp), _) if card(lp) == Card.Many && !lp.suspends =>
        lab("zip:step-left-inside-right")(pushLinear[B, A, C](r, lp, (b, a) => f(a, b)))
      case (_, Linear(rp)) if card(rp) == Card.Many && !rp.suspends =>
        lab("zip:step-right-inside-left")(pushLinear[A, B, C](l, rp, f))
      // Otherwise linearise only the shallower side (a pull) and let the other side drive.
      case _ =>
        if depth(r) <= depth(l) then lab("zip:pull-right")(drivePull[A, B, C](l, pull(r), f))
        else lab("zip:pull-left")(drivePull[B, A, C](r, pull(l), (b, a) => f(a, b)))

  /** How deeply nested a stream is: the cost of linearising it. */
  private def depth(s: StStream[?]): Int = s match
    case Linear(p)       => if p.composite then 1 else 0
    case _: Nested[?, ?] => 2

  /** Adds `cond` to the termination of the innermost loop only. */
  private def innermostTermination[A](s: StStream[A], cond: () => Expr[Boolean]): StStream[A] = s match
    case Linear(p) =>
      // `cond` goes last: it is only evaluated (e.g. a pull filled) when the loop would step.
      forUnfold(p) match
        case u: UnfoldP[A, s] =>
          Linear(derived(UnfoldP[A, s](u.init, st => '{ ${ u.term(st) } && ${ cond() } }, u.card, u.step, u.suspends)(using u.tpe), u))
    case n: Nested[A, b] => Nested[A, b](n.p, x => innermostTermination(n.f(x), cond))(using n.tpe)

  /** `drive` keeps its loops; `lin` (exactly one element per step, never suspends) is stepped once
   *  per driving element, and its termination stops every driving loop. */
  private def pushLinear[D, L, C: Type](drive: StStream[D], lin: Producer[L], f: (Expr[D], Expr[L]) => Expr[C]): StStream[C] =
    forUnfold(lin) match
      case u: UnfoldP[L, s] =>
        var st: Option[s] = None
        val init = u.init { x => st = Some(x); unit }
        val driven = moreTermination(drive, () => u.term(st.get))
        withOuterInit(mapRaw[D, C](driven, d => k => u.step(st.get)(x => letIn(f(d, x))(k))), () => init)

  /** `drive` keeps its loops; the pulled side is filled just before the innermost driving step
   *  (so a suspension never loses a driving element) and its end stops every driving loop. */
  private def drivePull[D, P, C: Type](drive: StStream[D], pl: Pull[P], f: (Expr[D], Expr[P]) => Expr[C]): StStream[C] =
    val notEnded = () => '{ !${ pl.ended.get } }
    val ready = () => '{ ${ pl.fill }; ${ pl.have.get } }
    val driven = innermostTermination(moreTermination(drive, notEnded), ready)
    withOuterInit(mapRaw[D, C](driven, d => k => seq(pl.have := '{ false }, letIn(f(d, pl.cur.get))(k))), () => pl.init)

  /** Tumbling windows of `size` elements, each folded with `op` starting from `z`. */
  def windowFold[A, S: Type](s: StStream[A], size: Expr[Int], z: Expr[S], op: (Expr[S], Expr[A]) => Expr[S]): StStream[S] =
    val sz = alloc[Int]("size")
    val zv = alloc[S]("z")
    val acc = alloc[S]("wacc")
    val cnt = alloc[Int]("cnt")
    val upEnded = alloc[Boolean]("upEnded")
    val prep = flags(s, List(() => '{ ${ cnt.get } < ${ sz.get } && !${ upEnded.get } }),
      a => seq(acc := op(acc.get, a), inc(cnt)), upEnded := '{ true }, pullTicks)
    val label = if describing then s"windowFold(${StStream.shape(s)})" else "windowFold"
    Linear(labeled(UnfoldP[S, Unit](
      k => seq(sz := '{ Math.max($size, 1) }, zv := z, acc := zv.get, cnt := '{ 0 }, upEnded := '{ false }, prep.init, k(())),
      _ => '{ !${ upEnded.get } || ${ cnt.get } > 0 },
      Card.AtMost1,
      _ => k =>
        val emit = bind("w", acc.get)(w => seq(acc := zv.get, cnt := '{ 0 }, k(w)))
        seq(candidate(prep.loop), '{ if (${ cnt.get } > 0) $emit }),
      suspends(s)), label).tap(_.composite = true))

  // ------------------------------------------------------------------ backends

  /** Init code (run once, before the loop) and the loop itself. */
  final case class Prepared(init: Expr[Unit], loop: Expr[Unit])

  private def whileLoop(guard: List[() => Expr[Boolean]], body: Expr[Unit]): Expr[Unit] =
    guard.map(_()).reduceOption((a, b) => '{ $a && $b }) match
      case None    => '{ while (true) $body }
      case Some(g) => '{ while ($g) $body }

  /** Resumable loops: run `s` while `guard` holds, feeding `k`; run `onEnd` once on exhaustion
   *  (which must make `guard` false, or leave the method). */
  def flags[A](s: StStream[A], guard: List[() => Expr[Boolean]], k: Expr[A] => Expr[Unit],
               onEnd: => Expr[Unit], ticks: Ticks): Prepared = s match
    case Linear(p) =>
      forUnfold(p) match
        case u: UnfoldP[A, st] =>
          var state: Option[st] = None
          val init = u.init { x => state = Some(x); unit }
          val tick = if u.composite then fullCheck(ticks) else ticks.inner()
          val body = '{ if (${ u.term(state.get) }) { $tick; ${ u.step(state.get)(k) } } else $onEnd }
          Prepared(init, whileLoop(guard, body))
    case n: Nested[A, b] =>
      given Type[b] = n.p.tpe
      val active = alloc[Boolean]("active")
      val xv = alloc[b]("x")
      val inner = flags(n.f(xv.get), guard :+ (() => active.get), k, active := '{ false }, ticks)
      forUnfold(n.p) match
        case u: UnfoldP[b, st] =>
          var state: Option[st] = None
          val init = seq(active := '{ false }, u.init { x => state = Some(x); unit })
          val innerLoop = candidate(inner.loop)
          val body = '{
            if (${ active.get }) $innerLoop
            else if (${ u.term(state.get) }) {
              ${ if u.composite then unit else ticks.outer() }
              ${ u.step(state.get)(x => seq(xv := x, inner.init, active := '{ true })) }
            } else $onEnd
          }
          Prepared(init, whileLoop(guard, body))

  /** Kiselyov's `fold_raw`: plain nested loops (direct mode). Includes init code. */
  def nest[A](s: StStream[A], k: Expr[A] => Expr[Unit]): Expr[Unit] = s match
    case Linear(p)       => nestProd(p, k)
    case n: Nested[A, b] => nestProd(n.p, x => candidate(nest(n.f(x), k)))

  private def nestProd[A](p: Producer[A], k: Expr[A] => Expr[Unit]): Expr[Unit] = p match
    case f: ForP[A, s] =>
      val i = alloc[Int]("i")
      f.init { s => seq(i := '{ 0 }, '{ while (${ i.get } < ${ f.upb(s) }) { ${ f.index(s, i.get)(k) }; ${ inc(i) } } }) }
    case u: UnfoldP[A, s] =>
      u.init { s => '{ while (${ u.term(s) }) ${ u.step(s)(k) } } }

  // ------------------------------------------------------------------ lowering Pipe → StStream

  def lower[A](p: Pipe[A]): StStream[A] =
    given Type[A] = p.tpe
    p match
      case Pipe.SrcArray(arr) =>
        val av = alloc[Array[A]]("arr")
        Linear(labeled(ForP[A, Unit](k => seq(av := arr, k(())), _ => '{ ${ av.get }.length },
          (_, i) => k => letIn('{ ${ av.get }($i) })(k)), "array"))
      case Pipe.SrcRange(from, until) =>
        val base = alloc[Int]("from")
        val len = alloc[Int]("len")
        Linear(labeled(ForP[Int, Unit](k => seq(base := from, len := '{ $until - ${ base.get } }, k(())), _ => len.get,
          (_, i) => k => letIn('{ ${ base.get } + $i })(k)), "range")).asInstanceOf[StStream[A]]
      case Pipe.SrcPort(n) => portStream[A](n)
      case Pipe.MapP(up, f) => map(lower(up), f(_))
      case Pipe.FilterP(up, pr) => filter(lower(up), pr(_))
      case Pipe.FlatMapP(up, f) => flatMap(lower(up), x => lower(f(x)))
      case Pipe.TakeP(up, n) => take(lower(up), n)
      case Pipe.ZipWithP(l, r, f) => zipWith(lower(l), lower(r), f(_, _))
      case Pipe.WindowFoldP(up, size, z, op) => windowFold(lower(up), size, z, op(_, _))
      case c: Pipe.CollectP[a, b] => mapPartial[a, b](lower(c.up), (x, k) => c.pf(x, k), "collect")(using c.tpe)
      case m: Pipe.MapOptionP[a, b] => mapOption[a, b](lower(m.up), m.f(_))(using m.tpe)
      case Pipe.ZipWithIndexP(up) => zipWithIndex(lower(up)).asInstanceOf[StStream[A]]
      case sc: Pipe.ScanP[a, s] => scan[a, s](lower(sc.up), sc.z, sc.op(_, _))(using sc.tpe)
      case Pipe.DropP(up, n) => drop(lower(up), n)
      case m: Pipe.MapAccumP[a, s, b] => mapAccum[a, s, b](lower(m.up), m.z, m.f(_, _), m.out)(using m.stateTpe, m.tpe)
      case Pipe.TakeWhileP(up, pr) => takeWhile(lower(up), pr(_))
      case Pipe.DropWhileP(up, pr) => dropWhile(lower(up), pr(_))

  /** An input port: yields buffered elements, suspends with `NeedInput(n)` when the buffer is
   *  empty and the port has not ended. */
  private def portStream[A: Type](n: Int): StStream[A] = mode match
    case Mode.Direct => report.errorAndAbort("input ports are only available in segments")
    case m: Mode.Resumable =>
      val arr = m.ports(n).asInstanceOf[Expr[Array[A]]]
      val (pos, end, ended) = (s"pos$n", s"end$n", s"ended$n")
      Linear(labeled(UnfoldP[A, Unit](
        k => k(()),
        _ => '{ ${ fld[Int](pos) } < ${ fld[Int](end) } || !${ fld[Boolean](ended) } },
        Card.Many,
        _ => k => bind("p", fld[Int](pos)) { p =>
          seq('{ if ($p >= ${ fld[Int](end) }) ${ ret(Expr(n)) } }, setFld(pos, '{ $p + 1 }), bind("x", '{ $arr($p) })(k))
        },
        suspends = true), s"port$n"))

  // ------------------------------------------------------------------ whole programs

  /** Direct mode: a fused fold over arrays/ranges. */
  def directFold[A, S](sink: Pipe.FoldSink[A, S]): Expr[S] =
    given Type[S] = sink.tpe
    val s = lower(sink.up)
    val zv = alloc[S]("z")
    val acc = alloc[S]("acc")
    val loop = nest(s, a => acc := sink.op(acc.get, a))
    '{ ${ seq(zv := sink.z, acc := zv.get, loop) }; ${ acc.get } }

  /** Resumable mode: the body of `stepImpl`. */
  def segmentStep(sink: Pipe.Sink): Expr[Int] =
    val done = Expr(Status.Done)
    val (init, loop) = sink match
      case fs: Pipe.FoldSink[a, s] =>
        given Type[s] = fs.tpe
        val zv = alloc[s]("z")
        val acc = alloc[s]("acc")
        val s = lower(fs.up)
        val prep = flags(s, Nil, a => acc := fs.op(acc.get, a),
          seq(setFld("finished", '{ true }), setFld("result", '{ ${ acc.get }: Any }), ret(done)),
          Ticks(() => fuelTick(), () => fuelTick()))
        (seq(zv := fs.z, acc := zv.get, prep.init), prep.loop)
      case es: Pipe.EmitSink[a] =>
        given Type[a] = es.tpe
        val out = mode.asInstanceOf[Mode.Resumable].out.asInstanceOf[Expr[Array[a]]]
        val s = lower(es.up)
        val full = () => '{ if (${ fld[Int]("outLen") } >= ${ fld[Int]("maxOut") }) ${ ret(Expr(Status.Emit)) } }
        val prep = flags(s, Nil,
          a => bind("n", fld[Int]("outLen"))(n => seq('{ $out($n) = $a }, setFld("outLen", '{ $n + 1 }))),
          seq(setFld("finished", '{ true }), ret(done)),
          Ticks(() => fuelTick(), () => seq(fuelTick(), full()), full))
        (prep.init, prep.loop)
    '{
      ${ setFld("outLen", '{ 0 }) }
      if (${ fld[Boolean]("finished") }) ${ ret(done) }
      ${ setFld("fuel", fld[Int]("budget")) }
      if (!${ fld[Boolean]("started") }) {
        ${ setFld("started", '{ true }) }
        $init
      }
      $loop
      $done
    }

  // ------------------------------------------------------------------ measurement

  def treeSize(e: Expr[?]): Int =
    val acc = new TreeAccumulator[Int]:
      def foldTree(n: Int, t: Tree)(owner: Symbol): Int = foldOverTree(n + 1, t)(owner)
    acc.foldTree(0, e.asTerm)(Symbol.spliceOwner)

object Gen:
  enum Mode:
    case Direct
    /** `self` is the segment's `this` (a `q.reflect.Term`), `ports` the input arrays cast to
     *  their element types, `out` the output array. */
    case Resumable(self: Any, ports: Vector[Expr[Array[?]]], out: Expr[Array[?]])

  /** Which candidate regions to outline, plus sizes measured on a previous pass. */
  final class SplitPlan(val outline: Set[Int]):
    val sizes = mutable.Map.empty[Int, Int]
    val parent = mutable.Map.empty[Int, Int]

  /** Estimated JVM bytecode bytes per tree node. Measured ≈1.0 on the test pipelines (see
   *  MethodSizeSuite); 1.2 leaves a margin. */
  val BytesPerNode = 1.2
  /** Target size for a generated method; HotSpot refuses to JIT methods over 8000 bytes. */
  val MethodBudgetBytes = 6000

  /** Chooses regions to outline so that every method fits the budget (greedy, largest first). */
  def choose(total: Int, sizes: collection.Map[Int, Int], parent: collection.Map[Int, Int]): Set[Int] =
    val limit = (MethodBudgetBytes / BytesPerNode).toInt
    var outlined = Set.empty[Int]
    def ownerOf(id: Int): Int =
      var p = parent.getOrElse(id, -1)
      while p != -1 && !outlined(p) do p = parent.getOrElse(p, -1)
      p
    def eff(m: Int): Int =
      val own = if m == -1 then total else sizes(m)
      own - outlined.iterator.filter(c => ownerOf(c) == m).map(sizes).sum
    var progress = true
    while progress do
      progress = false
      (-1 :: outlined.toList).find(m => eff(m) > limit).foreach { m =>
        val cands = sizes.keys.filter(id => !outlined(id) && ownerOf(id) == m && (m == -1 || isDescendant(id, m, parent)))
        if cands.nonEmpty then
          outlined += cands.maxBy(sizes)
          progress = true
      }
    outlined

  private def isDescendant(id: Int, anc: Int, parent: collection.Map[Int, Int]): Boolean =
    var p = parent.getOrElse(id, -1)
    while p != -1 && p != anc do p = parent.getOrElse(p, -1)
    p == anc
