package iofuse.internal

import cats.effect.IO
import scala.quoted.*

private[iofuse] object Compiler:
  def fused[A: Type](io: Expr[IO[A]])(using Quotes): Expr[IO[A]] = Impl().run(io)
  def show[A: Type](io: Expr[IO[A]])(using Quotes): Expr[String] =
    import quotes.reflect.*
    Expr(fused(io).asTerm.show(using Printer.TreeShortCode))

/** Parses the block into [[IR]], then emits continuation-passing code: synchronous parts are
 *  direct code, an opaque IO ends the synchronous run with `io.flatMap(rest)`. */
private final class Impl(using Quotes):
  import quotes.reflect.*

  private val ioClass  = Symbol.requiredClass("cats.effect.IO")
  private val ioModule = Symbol.requiredModule("cats.effect.IO").moduleClass
  private def ioOf(t: TypeRepr): TypeRepr = ioClass.typeRef.appliedTo(t)

  // ---- IR -------------------------------------------------------------------------------------

  /** `tp` is the value type: the `A` of the `IO[A]` the node stands for. */
  private enum IR:
    def tp: TypeRepr
    case Pure(v: Term, tp: TypeRepr)
    case Delay(t: Term, tp: TypeRepr)
    case Raise(e: Term, tp: TypeRepr)
    case Opaque(io: Term, tp: TypeRepr)
    case Bind(io: IR, x: Option[Symbol], body: IR, tp: TypeRepr)
    case BindF(io: IR, f: Term, tp: TypeRepr)
    case Map(io: IR, x: Option[Symbol], body: Term, tp: TypeRepr)
    case MapF(io: IR, f: Term, tp: TypeRepr)
    case Stats(stats: List[Statement], e: IR, defs: Set[Symbol], tp: TypeRepr)
    case Cond(c: Term, a: IR, b: IR, tp: TypeRepr)
    case Switch(s: Term, cases: List[(Tree, Option[Term], IR)], tp: TypeRepr)
    case Handle(io: IR, x: Symbol, h: IR, tp: TypeRepr)
    case Attempt(io: IR, tp: TypeRepr)
    case Call(sym: Symbol, args: List[Term], tp: TypeRepr)

  private def elem(tpe: TypeRepr): Option[TypeRepr] = tpe.widen.dealias match
    case OrType(a, b) => for x <- elem(a); y <- elem(b) yield OrType(x, y)
    case t => t.baseType(ioClass) match
      case AppliedType(_, List(a)) => Some(a)
      case _ => None

  private def elemOf(t: Term): TypeRepr =
    elem(t.tpe).getOrElse(report.errorAndAbort(s"iofuse: not an IO: ${t.tpe.show}", t.pos))

  private def strip(t: Term): Term = t match
    case Inlined(_, Nil, e) => strip(e)
    case Typed(e, _)        => strip(e)
    case Block(Nil, e)      => strip(e)
    case _                  => t

  /** `qual.m[..](args)`: the selected method and the flattened value arguments. */
  private def call(t: Term): Option[(Select, List[Term])] = t match
    case Apply(f, as)    => call(f).map((s, bs) => (s, bs ++ as))
    case TypeApply(f, _) => call(f)
    case s: Select       => Some((s, Nil))
    case _               => None

  private object Static:
    def unapply(t: Term): Option[(String, List[Term])] = call(t).collect {
      case (s, as) if s.symbol.owner == ioModule => (s.name, as)
    }
  private object Inst:
    def unapply(t: Term): Option[(Term, String, List[Term])] = call(t).collect {
      case (s, as) if s.symbol.owner == ioClass => (s.qualifier, s.name, as)
    }

  private def lambda(f: Term): Option[(Symbol, Term)] = strip(f) match
    case b: Block => Lambda.unapply(b).collect { case (List(p), body) => (p.symbol, body) }
    case _ => None

  private def isRef(t: Term, x: Symbol): Boolean = strip(t) match
    case i: Ident => i.symbol == x
    case _ => false

  /** A local def we compile: one parameter list, returns an IO. */
  private def loopable(d: DefDef): Boolean =
    d.rhs.isDefined && elem(d.returnTpt.tpe).isDefined && (d.paramss match
      case List(c: TermParamClause) => !c.isGiven && !c.isImplicit
      case _ => false)

  private def supported(s: Statement): Boolean = s match
    case _: ValDef | _: Term | _: Import => true
    case d: DefDef => loopable(d)
    case _ => false

  private def fresh(name: String, tp: TypeRepr) =
    Symbol.newVal(Symbol.spliceOwner, name, tp, Flags.EmptyFlags, Symbol.noSymbol)

  private def parse(t0: Term)(using defs: Set[Symbol]): IR =
    val t = strip(t0)
    val tp = elemOf(t)
    t match
      case Inlined(_, bs, e) => IR.Stats(bs, parse(e), defs, tp) // bindings are vals
      case Block(stats, e) if !stats.forall(supported) => IR.Opaque(t, tp)
      case Block(stats, e) =>
        val ds = defs ++ stats.collect { case d: DefDef if loopable(d) => d.symbol }
        IR.Stats(stats, parse(e)(using ds), ds, tp)
      case If(c, a, b)  => IR.Cond(c, parse(a), parse(b), tp)
      case Match(s, cs) => IR.Switch(s, cs.map(c => (c.pattern, c.guard, parse(c.rhs))), tp)
      case Static("pure", List(v))                => IR.Pure(v, tp)
      case Static("unit", Nil)                    => IR.Pure('{ () }.asTerm, tp)
      case Static("delay" | "apply", List(th))    => IR.Delay(th, tp)
      case Static("raiseError", List(e))          => IR.Raise(e, tp)
      case Static("defer", List(th))              => parse(th)
      case Inst(r, "flatMap", List(f)) => lambda(f) match
        case Some((x, b)) => IR.Bind(parse(r), Some(x), parse(b), tp)
        case None         => IR.BindF(parse(r), f, tp)
      case Inst(r, "map", List(f)) => lambda(f) match
        case Some((x, b)) if isRef(b, x) => parse(r) // `yield r`: keeps a recursive call in tail position
        case Some((x, b)) => IR.Map(parse(r), Some(x), b, tp)
        case None         => IR.MapF(parse(r), f, tp)
      case Inst(r, "*>" | ">>" | "productR", List(b)) => IR.Bind(parse(r), None, parse(b), tp)
      case Inst(r, "<*" | "productL", List(b)) =>
        val x = fresh("a", tp)
        val pb = parse(b)
        IR.Bind(parse(r), Some(x), IR.Map(pb, None, Ref(x), tp), tp)
      case Inst(r, "as", List(v)) => IR.Map(parse(r), None, v, tp)
      case Inst(r, "void", Nil)   => IR.Map(parse(r), None, '{ () }.asTerm, tp)
      case Inst(r, "attempt", Nil) => IR.Attempt(parse(r), tp)
      case Inst(r, "handleErrorWith", List(f)) => lambda(f) match
        case Some((x, b)) => IR.Handle(parse(r), x, parse(b), tp)
        case None =>
          val x = fresh("e", TypeRepr.of[Throwable])
          IR.Handle(parse(r), x, IR.Opaque(Select.overloaded(f, "apply", Nil, List(Ref(x))), tp), tp)
      case Apply(fn: Ident, args) if defs(fn.symbol) => IR.Call(fn.symbol, args, tp)
      case _ => IR.Opaque(t, tp)

  private def isSync(ir: IR): Boolean = ir match
    case _: IR.Pure | _: IR.Delay | _: IR.Raise          => true
    case _: IR.Opaque | _: IR.Call | _: IR.BindF         => false
    case IR.Bind(io, _, b, _)                            => isSync(io) && isSync(b)
    case IR.Map(io, _, _, _)                             => isSync(io)
    case IR.MapF(io, _, _)                               => isSync(io)
    case IR.Stats(_, e, _, _)                            => isSync(e)
    case IR.Cond(_, a, b, _)                             => isSync(a) && isSync(b)
    case IR.Switch(_, cs, _)                             => cs.forall(c => isSync(c._3))
    case IR.Handle(io, _, h, _)                          => isSync(io) && isSync(h)
    case IR.Attempt(io, _)                               => isSync(io)

  // ---- emission -------------------------------------------------------------------------------

  /** A compiled local def's loop state: the parameters as vars, and the iteration counter. */
  private final case class Loop(sym: Symbol, vars: List[Symbol], n: Symbol)

  /** owner: the symbol owning new definitions. r: the value type of the IO the code returns.
   *  async: inside a generated lambda (so a tail call must call the def, not jump).
   *  inDef: inside a compiled def (non-tail calls go through IO.defer, for stack safety).
   *  sub: lambda / def parameters replaced by vals. */
  private final case class Ctx(owner: Symbol, r: TypeRepr, loop: Option[Loop], async: Boolean,
                               inDef: Boolean, sub: Map[Symbol, Symbol]):
    def bind(x: Option[Symbol], v: Term): Ctx = x.fold(this)(s => copy(sub = sub + (s -> v.symbol)))

  /** What happens to a value: return it as the result, or run more code. */
  private enum K:
    case Ret
    case Fn(f: (Term, Ctx) => Term)

  private var counter = 0
  private def name(s: String) = { counter += 1; s"$s$$$counter" }

  private final class Subst(m: Map[Symbol, Symbol]) extends TreeMap:
    override def transformTerm(t: Term)(owner: Symbol): Term = t match
      case i: Ident if m.contains(i.symbol) => Ref(m(i.symbol))
      case _ => super.transformTerm(t)(owner)

  private def user(t: Term)(using c: Ctx): Term =
    val s = if c.sub.isEmpty then t else Subst(c.sub).transformTerm(t)(c.owner)
    s.changeOwner(c.owner)

  private def withT[R](tp: TypeRepr)(f: [t] => Type[t] ?=> R): R = tp.asType match
    case '[t] => f[t]

  // Splicing a tree into a quote re-owns its definitions to the macro's owner: put them back.
  private def pure(v: Term, tp: TypeRepr)(using c: Ctx): Term =
    withT(tp)([t] => (_: Type[t]) ?=> '{ IO.pure[t](${ v.asExprOf[t] }) }.asTerm).changeOwner(c.owner)
  private def defer(io: Term, tp: TypeRepr)(using c: Ctx): Term =
    withT(tp)([t] => (_: Type[t]) ?=> '{ IO.defer[t](${ io.asExprOf[IO[t]] }) }.asTerm).changeOwner(c.owner)
  private def raise(e: Term)(using c: Ctx): Term = '{ throw ${ e.asExprOf[Throwable] } }.asTerm.changeOwner(c.owner)

  private def bindVal(rhs: Term, tp: TypeRepr)(body: (Term, Ctx) => Term)(using c: Ctx): Term =
    val s = Symbol.newVal(c.owner, name("v"), tp, Flags.EmptyFlags, Symbol.noSymbol)
    Block(List(ValDef(s, Some(rhs.changeOwner(s)))), body(Ref(s), c))

  private def lam(param: TypeRepr, res: TypeRepr)(body: (Term, Ctx) => Term)(using c: Ctx): Term =
    val mt = MethodType(List(name("x")))(_ => List(param), _ => ioOf(res))
    Lambda(c.owner, mt, (meth, ps) => body(ps.head.asInstanceOf[Term], c.copy(owner = meth, async = true, r = res)))

  private def applyF(f: Term, v: Term): Term = Select.overloaded(f, "apply", Nil, List(v))

  /** Value code (sync IR only). */
  private def direct(ir: IR)(using c: Ctx): Term = ir match
    case IR.Pure(v, _)  => user(v)
    case IR.Delay(t, _) => user(t)
    case IR.Raise(e, _) => raise(user(e))
    case IR.Bind(io, x, b, _) => bindVal(direct(io), io.tp)((v, c2) => direct(b)(using c2.bind(x, v)))
    case IR.Map(io, x, b, _)  => bindVal(direct(io), io.tp)((v, c2) => user(b)(using c2.bind(x, v)))
    case IR.MapF(io, f, _)    => bindVal(direct(io), io.tp)((v, c2) => applyF(user(f)(using c2), v))
    case IR.Stats(ss, e, ds, _) => stats(ss, ds)(direct(e))
    case IR.Cond(cnd, a, b, _)  => If(user(cnd), direct(a), direct(b))
    case IR.Switch(s, cs, _) =>
      Match(user(s), cs.map((p, g, b) => CaseDef(p, g.map(user), direct(b)))).changeOwner(c.owner)
    case IR.Handle(io, x, h, _) => tryCatch(direct(io))(c2 => direct(h)(using c2), Some(x))
    case IR.Attempt(io, tp) =>
      val a = io.tp
      withT(a)([t] => (_: Type[t]) ?=>
        tryCatch('{ Right(${ direct(io).asExprOf[t] }) }.asTerm.changeOwner(c.owner))(
          c2 => '{ Left(${ Ref(c2.sub(errKey)).asExprOf[Throwable] }) }.asTerm, Some(errKey)))
    case _ => report.errorAndAbort(s"iofuse: not synchronous: $ir")

  private val errKey = fresh("err", TypeRepr.of[Throwable])

  private def tryCatch(body: Term)(handler: Ctx => Term, x: Option[Symbol])(using c: Ctx): Term =
    val e = Symbol.newBind(c.owner, name("e"), Flags.EmptyFlags, TypeRepr.of[Throwable])
    val guard = '{ scala.util.control.NonFatal(${ Ref(e).asExprOf[Throwable] }) }.asTerm
    Try(body, List(CaseDef(Bind(e, Wildcard()), Some(guard), handler(c.bind(x, Ref(e))))), None)

  private def applyK(k: K, v: Term, tp: TypeRepr)(using c: Ctx): Term = k match
    case K.Ret   => pure(v, tp)
    case K.Fn(f) => bindVal(v, tp)(f)

  /** Run `io` (of value type tp), then k. */
  private def suspend(io: Term, tp: TypeRepr, k: K)(using c: Ctx): Term = k match
    case K.Ret   => io
    case K.Fn(f) => Select.overloaded(io, "flatMap", List(c.r), List(lam(tp, c.r)(f)))

  /** A continuation used by several branches becomes a local def (a join point). */
  private def join(k: K, tp: TypeRepr)(body: K => Term)(using c: Ctx): Term = k match
    case K.Ret => body(K.Ret)
    case K.Fn(f) =>
      val j = Symbol.newMethod(c.owner, name("join"), MethodType(List("v"))(_ => List(tp), _ => ioOf(c.r)))
      val dd = DefDef(j, { case List(List(v: Term)) => Some(f(v, c.copy(owner = j)).changeOwner(j)); case _ => None })
      Block(List(dd), body(K.Fn((v, _) => Apply(Ref(j), List(v)))))

  /** Code returning IO[c.r] (or null inside a loop: "continue with the updated vars"). */
  private def cps(ir: IR, k: K)(using c: Ctx): Term =
    if isSync(ir) then
      k match
        case K.Ret   => pure(direct(ir), ir.tp)
        case K.Fn(f) => bindVal(direct(ir), ir.tp)(f)
    else ir match
      case IR.Opaque(t, tp)          => suspend(user(t), tp, k)
      case IR.Call(sym, args, tp)    => callDef(sym, args, tp, k)
      case IR.Bind(io, x, b, _)      => cps(io, K.Fn((v, c2) => cps(b, k)(using c2.bind(x, v))))
      case IR.BindF(io, f, tp)       => cps(io, K.Fn((v, c2) => suspend(applyF(user(f)(using c2), v), tp, k)(using c2)))
      case IR.Map(io, x, b, tp)      => cps(io, K.Fn((v, c2) => { given Ctx = c2.bind(x, v); applyK(k, user(b), tp) }))
      case IR.MapF(io, f, tp)        => cps(io, K.Fn((v, c2) => { given Ctx = c2; applyK(k, applyF(user(f), v), tp) }))
      case IR.Stats(ss, e, ds, _)    => stats(ss, ds)(cps(e, k))
      case IR.Cond(cnd, a, b, tp)    => join(k, tp)(k2 => If(user(cnd), cps(a, k2), cps(b, k2)))
      case IR.Switch(s, cs, tp) =>
        join(k, tp)(k2 => Match(user(s), cs.map((p, g, b) => CaseDef(p, g.map(user), cps(b, k2)))).changeOwner(c.owner))
      case IR.Handle(io, x, h, tp) =>
        // Errors from opaque parts arrive in the error channel: keep a real handleErrorWith.
        val region = c.copy(loop = None)
        val inner = defer(cps(io, K.Ret)(using region.copy(r = io.tp)), io.tp)
        val handled = Select.overloaded(inner, "handleErrorWith", List(tp),
          List(lam(TypeRepr.of[Throwable], tp)((e, c2) => cps(h, K.Ret)(using c2.copy(loop = None).bind(Some(x), e)))))
        suspend(handled, tp, k)
      case IR.Attempt(io, tp) =>
        val inner = defer(cps(io, K.Ret)(using c.copy(loop = None, r = io.tp)), io.tp)
        suspend(Select.unique(inner, "attempt"), tp, k)
      case _ => report.errorAndAbort(s"iofuse: unexpected $ir")

  private def callDef(sym0: Symbol, args0: List[Term], tp: TypeRepr, k: K)(using c: Ctx): Term =
    val sym = c.sub.getOrElse(sym0, sym0)
    val args = args0.map(user)
    c.loop match
      case Some(l) if l.sym == sym && k == K.Ret =>
        if c.async then Apply(Ref(sym), args) // resumed after an opaque step: start a new loop
        else
          // Jump: evaluate the arguments, then either continue the while loop or cede.
          def go(as: List[Term], vs: List[Term])(using c2: Ctx): Term = as match
            case a :: rest => bindVal(a, a.tpe.widen)((v, c3) => go(rest, vs :+ v)(using c3))
            case Nil =>
              val n = Ref(l.n).asExprOf[Int]
              val cede = Select.overloaded('{ IO.cede }.asTerm, "flatMap", List(c.r),
                List(lam(TypeRepr.of[Unit], c.r)((_, _) => Apply(Ref(sym), vs))))
              val cont = Block(l.vars.zip(vs).map((x, v) => Assign(Ref(x), v)), Literal(NullConstant()))
              Block(List(Assign(Ref(l.n), '{ $n + 1 }.asTerm)),
                If('{ $n >= iofuse.IOFuse.YieldEvery }.asTerm, cede, cont))
          go(args, Nil)
      case _ =>
        val call = Apply(Ref(sym), args)
        suspend(if c.inDef then defer(call, tp) else call, tp, k)

  /** User statements followed by code. Their vals and defs get fresh symbols owned by the current
   *  owner (a definition can't be re-owned apart from its uses); references are substituted. */
  private def stats(ss: List[Statement], defs: Set[Symbol])(rest: Ctx ?=> Term)(using c0: Ctx): Term =
    val fresh: List[(Statement, Option[Symbol])] = ss.map {
      case v: ValDef =>
        val f = Seq(Flags.Mutable, Flags.Lazy).filter(v.symbol.flags.is).foldLeft(Flags.EmptyFlags)(_ | _)
        (v, Some(Symbol.newVal(c0.owner, v.name, v.tpt.tpe, f, Symbol.noSymbol)))
      case d: DefDef =>
        val ps = d.termParamss.head.params
        val mt = MethodType(ps.map(_.name))(_ => ps.map(_.tpt.tpe), _ => d.returnTpt.tpe)
        (d, Some(Symbol.newMethod(c0.owner, d.name, mt)))
      case s => (s, None)
    }
    val c = c0.copy(sub = c0.sub ++ fresh.collect { case (d: Definition, Some(n)) => d.symbol -> n })
    val out = fresh.map {
      case (v: ValDef, Some(n)) => ValDef(n, v.rhs.map(r => user(r)(using c).changeOwner(n)))
      case (d: DefDef, Some(n)) => compileDef(d, n, defs)(using c)
      case (t: Term, _)         => user(t)(using c)
      case (s, _)               => s
    }
    Block(out, rest(using c))

  /** `def f(p..): IO[X] = body` becomes
   *  `{ var p'..; var n = 0; var res = null; while res eq null do { val p = p'..; res = body' }; res }`
   *  where a synchronous self-call in tail position assigns the vars and yields null. */
  private def compileDef(d: DefDef, sym: Symbol, defs: Set[Symbol])(using c: Ctx): DefDef =
    val x = elem(d.returnTpt.tpe).get
    val params = d.termParamss.head.params
    DefDef(sym, {
      case List(ps) =>
        val vars = params.map(p => Symbol.newVal(sym, name(p.name), p.tpt.tpe, Flags.Mutable, Symbol.noSymbol))
        val n    = Symbol.newVal(sym, name("n"), TypeRepr.of[Int], Flags.Mutable, Symbol.noSymbol)
        val res  = Symbol.newVal(sym, name("res"), ioOf(x), Flags.Mutable, Symbol.noSymbol)
        val its  = params.map(p => Symbol.newVal(sym, name(p.name), p.tpt.tpe, Flags.EmptyFlags, Symbol.noSymbol))
        val lc = Ctx(sym, x, Some(Loop(sym, vars, n)), async = false, inDef = true,
                     sub = c.sub ++ params.map(_.symbol).zip(its))
        val body = Block(its.zip(vars).map((i, v) => ValDef(i, Some(Ref(v)))),
                         Assign(Ref(res), cps(parse(d.rhs.get)(using defs), K.Ret)(using lc)))
        val resE = Ref(res).asExprOf[AnyRef]
        Some(Block(
          vars.zip(ps).map((v, p) => ValDef(v, Some(Ref(p.symbol)))) ++
            List(ValDef(n, Some(Literal(IntConstant(0)))), ValDef(res, Some(Literal(NullConstant()))),
                 While('{ $resE eq null }.asTerm, body)),
          Ref(res)))
      case _ => None
    })

  def run[A: Type](io: Expr[IO[A]]): Expr[IO[A]] =
    val ir = parse(io.asTerm)(using Set.empty)
    ir match
      case _: IR.Opaque => io // nothing to fuse
      case _ =>
        val c = Ctx(Symbol.spliceOwner, TypeRepr.of[A], None, async = false, inDef = false, Map.empty)
        val body = cps(ir, K.Ret)(using c).asExprOf[IO[A]]
        '{ IO.defer[A]($body) }
