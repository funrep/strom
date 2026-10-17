package strom.effect.internal

import cats.effect.IO
import zio.{Exit, ZIO}
import scala.quoted.*

/** Thrown by fused ZIO code for a typed failure (`ZIO.fail`) raised in synchronous code. Each
 *  boundary back to the runtime turns it into a failure again; only `catchAll` / `either` catch it. */
final class Fail(val e: Any) extends RuntimeException(null, null, false, false)

private object Impl:
  /** `-Dstrom.effect.notes=off` on the compiler's JVM silences the compile-time notes. */
  def notesOff: Boolean = sys.props.get("strom.effect.notes").contains("off")
  /** `-Dstrom.effect.fuse=off` on the compiler's JVM compiles every block as written (for debugging:
   *  cats-effect's async traces then show every step). */
  def fuseOff: Boolean = sys.props.get("strom.effect.fuse").contains("off")

private[effect] object Compiler:
  /** See [[Impl]]'s `chunk`. */
  final val MaxNodes = 2500
  def fusedIO[A: Type](io: Expr[IO[A]])(using Quotes): Expr[IO[A]] =
    Impl(isZIO = false, quiet = false).run(io).asExprOf[IO[A]]
  def fusedZIO[R: Type, E: Type, A: Type](z: Expr[ZIO[R, E, A]])(using Quotes): Expr[ZIO[R, E, A]] =
    Impl(isZIO = true, quiet = false).run(z).asExprOf[ZIO[R, E, A]]
  // For the tests (strom.effect.internal.Testing): another yield interval, and gate = false to
  // compile every block and def, also where that saves nothing (the fuzzers).
  def fusedIOWith[A: Type](io: Expr[IO[A]], every: Expr[Int], gate: Expr[Boolean])(using Quotes): Expr[IO[A]] =
    Impl(isZIO = false, quiet = false, positive(every), gate.valueOrAbort).run(io).asExprOf[IO[A]]
  def fusedZIOWith[R: Type, E: Type, A: Type](z: Expr[ZIO[R, E, A]], every: Expr[Int], gate: Expr[Boolean])(using Quotes): Expr[ZIO[R, E, A]] =
    Impl(isZIO = true, quiet = false, positive(every), gate.valueOrAbort).run(z).asExprOf[ZIO[R, E, A]]
  def showIOWith[A: Type](io: Expr[IO[A]], every: Expr[Int])(using Quotes): Expr[String] =
    show(Impl(isZIO = false, quiet = true, positive(every)).run(io))
  private def positive(every: Expr[Int])(using Quotes): Int =
    val n = every.valueOrAbort
    if n < 1 then quotes.reflect.report.errorAndAbort("strom-effect: yieldEvery must be at least 1", every)
    n
  def showIO[A: Type](io: Expr[IO[A]])(using Quotes): Expr[String] = show(Impl(isZIO = false, quiet = true).run(io))
  def showZIO[R: Type, E: Type, A: Type](z: Expr[ZIO[R, E, A]])(using Quotes): Expr[String] =
    show(Impl(isZIO = true, quiet = true).run(z))
  def notesIO[A: Type](io: Expr[IO[A]])(using Quotes): Expr[List[String]] = notes(Impl(isZIO = false, quiet = true), io)
  def notesZIO[R: Type, E: Type, A: Type](z: Expr[ZIO[R, E, A]])(using Quotes): Expr[List[String]] =
    notes(Impl(isZIO = true, quiet = true), z)
  private def notes(impl: Impl, e: Expr[Any])(using Quotes): Expr[List[String]] =
    impl.run(e)
    Expr(impl.notes.toList)
  private def show(code: Expr[Any])(using Quotes): Expr[String] =
    import quotes.reflect.*
    val s = code.asTerm.show(using Printer.TreeShortCode)
    // A string constant holds at most 65535 bytes: longer code is joined at run time.
    if s.length <= 16000 then Expr(s)
    else '{ ${ Expr.ofList(s.grouped(16000).map(Expr(_)).toList) }.mkString }

/** Parses the block into [[IR]], then emits continuation-passing code: synchronous parts are
 *  direct code, an opaque effect ends the synchronous run with `effect.flatMap(rest)`. The effect
 *  type is a [[Backend]]: cats-effect IO or ZIO. */
/** gate: leave code that fusing would save nothing on as written (off only for the fuzzers, which
 *  test the compiler on all of their programs). */
private final class Impl(isZIO: Boolean, quiet: Boolean, every: Int = strom.effect.StromEffect.YieldEvery,
                         gate: Boolean = true)(using Quotes):
  import quotes.reflect.*

  /** What the compiler tells the user (as compile-time notes, unless quiet). */
  val notes = scala.collection.mutable.ListBuffer.empty[String]

  // ---- IR -------------------------------------------------------------------------------------

  /** The effect's environment and error types (ZIO's R and E; `Any` and `Throwable` for IO). */
  private final case class Fx(env: TypeRepr, err: TypeRepr)

  /** `tp` is the value type: the `A` of the effect the node stands for.
   *
   *  The plain code builds an effect before it runs it, and what it evaluates while building (the
   *  arguments of strict operators, opaque effect expressions, the statements of a block) can have
   *  effects of its own. A region is what is built in one go: the block, a lambda body, a by-name
   *  argument, a def body. `strict` marks the parts built with their region; [[Lazy]] starts a new
   *  one. See [[Hoist]]. */
  private enum IR:
    def tp: TypeRepr
    case Pure(v: Term, tp: TypeRepr)
    case Delay(t: Term, tp: TypeRepr)
    case Raise(e: Term, strict: Boolean, tp: TypeRepr) // e: the Throwable to throw; strict: built with the region
    case Opaque(io: Term, tp: TypeRepr)
    case Bind(io: IR, x: Option[Symbol], body: IR, strict: Boolean, tp: TypeRepr) // strict: `a *> b`, not `a.flatMap(x => b)`
    case BindF(io: IR, f: Term, tp: TypeRepr)
    case Map(io: IR, x: Option[Symbol], body: Term, strict: Boolean, tp: TypeRepr) // strict: `io.as(v)` with a by-value v
    case MapF(io: IR, f: Term, tp: TypeRepr)
    case Stats(stats: List[Statement], e: IR, defs: Set[Symbol], tp: TypeRepr)
    case Cond(c: Term, a: IR, b: IR, src: Term, tp: TypeRepr) // src: the user's `if`
    case Switch(s: Term, cases: List[(Tree, Option[Term], IR)], src: Term, tp: TypeRepr)
    case Handle(io: IR, x: Symbol, h: IR, fxIn: Fx, tp: TypeRepr)
    case Attempt(io: IR, fxIn: Fx, tp: TypeRepr)
    case Call(sym: Symbol, args: List[Term], plan: Option[Term], tp: TypeRepr) // plan: what the callee's head built, if built ahead
    case Lazy(io: IR, tp: TypeRepr) // a by-name argument (`defer`, `>>`, ZIO's `*>`): built when it runs
    /** A `match` that [[Hoist]] has already decided: `which` is the number of the case taken. */
    case Chosen(which: Term, cases: List[IR], tp: TypeRepr)

  /** The `Trace` the user's call passed (ZIO), by node: generated calls reuse it. */
  private val traces = new java.util.IdentityHashMap[IR, Term]
  private def traceOf(ir: IR): Option[Term] = Option(traces.get(ir))
  private def keepTrace(from: IR, to: IR): IR =
    if (to ne from) && traces.containsKey(from) && !traces.containsKey(to) then traces.put(to, traces.get(from))
    if (to ne from) && callPos.containsKey(from) then callPos.put(to, callPos.get(from))
    to

  /** Where a call of a local def is written, and the defs of each block with the defs in scope. */
  private val callPos = new java.util.IdentityHashMap[IR, Position]
  private val defSrc = scala.collection.mutable.Map.empty[Symbol, (DefDef, Set[Symbol])]
  private val bodies = scala.collection.mutable.Map.empty[Symbol, IR]

  // ---- backends -------------------------------------------------------------------------------

  /** What differs between effect types: names, constructors, and error handling. */
  private trait Backend:
    def cls: Symbol
    def module: Symbol
    def split(t: TypeRepr): Option[(Fx, TypeRepr)]
    def effect(fx: Fx, a: TypeRepr): TypeRepr
    /** Canonical names: delay, unit, pure, raise, fail (typed), attemptDelay, defer (static);
     *  flatMap, map, then, before, as, void, attempt, handle (instance). `thenLazy` and
     *  `beforeLazy` take their argument by name. */
    def staticOp(name: String): Option[String]
    def instOp(name: String): Option[String]
    def failWith(e: Term): Term
    def attemptThunk(t: Term, tp: TypeRepr): Term
    def pure(v: Term, a: TypeRepr)(using Ctx): Term
    def defer(io: Term, a: TypeRepr)(using Ctx): Term
    /** Are `raise`'s and `as`'s arguments by value (evaluated when the effect is built)? */
    def strict: Boolean
    def flatMap(io: Term, a: TypeRepr, k: Term, trace: Option[Term])(using Ctx): Term
    def cede(using Ctx): Term
    def errType(fx: Fx): TypeRepr
    /** A catch case for what `handle` / `attempt` catch; the handler gets the error value. */
    def catchCase(fxIn: Fx)(handler: (Term, Ctx) => Term)(using Ctx): CaseDef
    def handled(inner: Term, a: TypeRepr, fxIn: Fx, h: Term, tp: TypeRepr, trace: Option[Term])(using Ctx): Term
    def attempted(inner: Term, a: TypeRepr, fxIn: Fx, trace: Option[Term])(using Ctx): Term
    /** Wraps code that the runtime calls (a thunk, a continuation, a loop run). */
    def boundary(body: Term, a: TypeRepr)(using Ctx): Term

  private def withT[R](tp: TypeRepr)(f: [t] => Type[t] ?=> R): R = tp.asType match
    case '[t] => f[t]
  private def withFx[R](fx: Fx, a: TypeRepr)(f: [r, e, x] => (Type[r], Type[e], Type[x]) ?=> R): R =
    ((fx.env.asType, fx.err.asType, a.asType): @unchecked) match
      case ('[r], '[e], '[x]) => f[r, e, x]

  /** Splicing a tree into a quote re-owns its definitions to the macro's owner: put them back. */
  extension (t: Term) private def owned(using c: Ctx): Term = t.changeOwner(c.owner)

  private object IOBackend extends Backend:
    val cls = Symbol.requiredClass("cats.effect.IO")
    val module = Symbol.requiredModule("cats.effect.IO").moduleClass
    def split(t: TypeRepr) = t.baseType(cls) match
      case AppliedType(_, List(a)) => Some((Fx(TypeRepr.of[Any], TypeRepr.of[Throwable]), a))
      case _ => None
    def effect(fx: Fx, a: TypeRepr) = cls.typeRef.appliedTo(a)
    def staticOp(n: String) = n match
      case "pure" | "unit" | "defer" => Some(n)
      case "delay" | "apply" => Some("delay")
      case "raiseError" => Some("raise")
      case _ => None
    def instOp(n: String) = n match
      case "flatMap" | "map" | "as" | "void" | "attempt" => Some(n)
      case "*>" | "productR" => Some("then")
      case ">>" => Some("thenLazy")
      case "<*" | "productL" => Some("before")
      case "handleErrorWith" => Some("handle")
      case _ => None
    def failWith(e: Term) = e
    def attemptThunk(t: Term, tp: TypeRepr) = t
    def pure(v: Term, a: TypeRepr)(using Ctx) =
      withT(a)([t] => (_: Type[t]) ?=> '{ IO.pure[t](${ v.asExprOf[t] }) }.asTerm).owned
    def defer(io: Term, a: TypeRepr)(using Ctx) =
      withT(a)([t] => (_: Type[t]) ?=> '{ IO.defer[t](${ io.asExprOf[IO[t]] }) }.asTerm).owned
    val strict = true
    def flatMap(io: Term, a: TypeRepr, k: Term, trace: Option[Term])(using c: Ctx) =
      Select.overloaded(io, "flatMap", List(c.r), List(k))
    def cede(using Ctx) = '{ IO.cede }.asTerm
    def errType(fx: Fx) = TypeRepr.of[Throwable]
    def catchCase(fxIn: Fx)(handler: (Term, Ctx) => Term)(using c: Ctx) =
      val e = Symbol.newBind(c.owner, name("e"), Flags.EmptyFlags, TypeRepr.of[Throwable])
      val guard = '{ scala.util.control.NonFatal(${ Ref(e).asExprOf[Throwable] }) }.asTerm
      CaseDef(Bind(e, Wildcard()), Some(guard), handler(Ref(e), c))
    def handled(inner: Term, a: TypeRepr, fxIn: Fx, h: Term, tp: TypeRepr, trace: Option[Term])(using Ctx) =
      Select.overloaded(defer(inner, a), "handleErrorWith", List(tp), List(h))
    def attempted(inner: Term, a: TypeRepr, fxIn: Fx, trace: Option[Term])(using Ctx) = Select.unique(defer(inner, a), "attempt")
    def boundary(body: Term, a: TypeRepr)(using Ctx) = body // a throw already becomes an IO error

  private object ZIOBackend extends Backend:
    val cls = Symbol.requiredClass("zio.ZIO")
    val module = Symbol.requiredModule("zio.ZIO").moduleClass
    def split(t: TypeRepr) = t.baseType(cls) match
      case AppliedType(_, List(r, e, a)) => Some((Fx(r, e), a))
      case _ => None
    def effect(fx: Fx, a: TypeRepr) = cls.typeRef.appliedTo(List(fx.env, fx.err, a))
    def staticOp(n: String) = n match
      case "unit" => Some(n)
      case "succeed" => Some("delay")
      case "attempt" => Some("attemptDelay")
      case "fail" => Some("fail")
      case "die" => Some("raise")
      case "suspendSucceed" => Some("defer")
      case _ => None
    def instOp(n: String) = n match
      case "flatMap" | "map" | "as" => Some(n)
      case "*>" | "zipRight" => Some("thenLazy")
      case "<*" | "zipLeft" => Some("beforeLazy")
      case "unit" => Some("void")
      case "either" => Some("attempt")
      case "catchAll" => Some("handle")
      case _ => None
    def failWith(e: Term) = '{ new Fail(${ e.asExpr }) }.asTerm
    def attemptThunk(t: Term, tp: TypeRepr) = withT(tp)([t] => (_: Type[t]) ?=>
      '{ try ${ t.asExprOf[t] } catch { case e if scala.util.control.NonFatal(e) => throw new Fail(e) } }.asTerm)
    def pure(v: Term, a: TypeRepr)(using Ctx) =
      withT(a)([t] => (_: Type[t]) ?=> '{ Exit.succeed[t](${ v.asExprOf[t] }) }.asTerm).owned
    def defer(io: Term, a: TypeRepr)(using c: Ctx) =
      withFx(c.fx, a)([r, e, x] => (_: Type[r], _: Type[e], _: Type[x]) ?=>
        '{ ZIO.suspendSucceed[r, e, x](${ io.asExprOf[ZIO[r, e, x]] }) }.asTerm).owned
    val strict = false // ZIO.fail, ZIO.die and `as` take their argument by name
    def flatMap(io: Term, a: TypeRepr, k: Term, trace: Option[Term])(using c: Ctx) =
      withFx(c.fx, c.r)([r, e, b] => (_: Type[r], _: Type[e], _: Type[b]) ?=> withT(a)([x] => (_: Type[x]) ?=>
        trace match
          case Some(tr) =>
            '{ ${ io.asExprOf[ZIO[r, e, x]] }.flatMap(${ k.asExprOf[x => ZIO[r, e, b]] })(using ${ tr.asExprOf[zio.Trace] }) }.asTerm
          case None =>
            '{ ${ io.asExprOf[ZIO[r, e, x]] }.flatMap(${ k.asExprOf[x => ZIO[r, e, b]] }) }.asTerm)).owned
    def cede(using Ctx) = '{ ZIO.yieldNow }.asTerm
    def errType(fx: Fx) = fx.err
    def catchCase(fxIn: Fx)(handler: (Term, Ctx) => Term)(using c: Ctx) =
      val f = Symbol.newBind(c.owner, name("f"), Flags.EmptyFlags, TypeRepr.of[Fail])
      val err = withT(fxIn.err)([e] => (_: Type[e]) ?=> '{ ${ Ref(f).asExprOf[Fail] }.e.asInstanceOf[e] }.asTerm)
      CaseDef(Bind(f, TypedOrTest(Wildcard(), TypeTree.of[Fail])), None, bindVal(err, fxIn.err)(handler))
    def handled(inner: Term, a: TypeRepr, fxIn: Fx, h: Term, tp: TypeRepr, trace: Option[Term])(using c: Ctx) =
      withFx(fxIn, a)([r, e, x] => (_: Type[r], _: Type[e], _: Type[x]) ?=>
        withFx(c.fx, tp)([r2, e2, y] => (_: Type[r2], _: Type[e2], _: Type[y]) ?=>
          trace match
            case Some(tr) =>
              val t = tr.asExprOf[zio.Trace]
              '{ ZIO.suspendSucceed(${ inner.asExprOf[ZIO[r, e, x]] })(using $t)
                   .catchAll(${ h.asExprOf[e => ZIO[r2, e2, y]] })(using zio.CanFail.canFail[e], $t) }.asTerm
            case None =>
              '{ ZIO.suspendSucceed(${ inner.asExprOf[ZIO[r, e, x]] }).catchAll(${ h.asExprOf[e => ZIO[r2, e2, y]] }) }.asTerm)).owned
    def attempted(inner: Term, a: TypeRepr, fxIn: Fx, trace: Option[Term])(using c: Ctx) =
      withFx(fxIn, a)([r, e, x] => (_: Type[r], _: Type[e], _: Type[x]) ?=>
        trace match
          case Some(tr) =>
            val t = tr.asExprOf[zio.Trace]
            '{ ZIO.suspendSucceed(${ inner.asExprOf[ZIO[r, e, x]] })(using $t).either(using zio.CanFail.canFail[e], $t) }.asTerm
          case None =>
            '{ ZIO.suspendSucceed(${ inner.asExprOf[ZIO[r, e, x]] }).either }.asTerm).owned
    def boundary(body: Term, a: TypeRepr)(using c: Ctx) =
      withFx(c.fx, a)([r, e, x] => (_: Type[r], _: Type[e], _: Type[x]) ?=>
        '{ try ${ body.asExprOf[ZIO[r, e, x]] } catch { case f: Fail => Exit.fail(f.e.asInstanceOf[e]) } }.asTerm).owned

  private val B: Backend = if isZIO then ZIOBackend else IOBackend

  private def split(tpe: TypeRepr): Option[(Fx, TypeRepr)] = tpe.widen.dealias match
    case OrType(a, b) => for (fa, x) <- split(a); (_, y) <- split(b) yield (fa, OrType(x, y))
    case t => B.split(t)
  private def elem(tpe: TypeRepr): Option[TypeRepr] = split(tpe).map(_._2)
  private def fxOf(t: Term): Fx = split(t.tpe).map(_._1).get

  private def elemOf(t: Term): TypeRepr =
    elem(t.tpe).getOrElse(report.errorAndAbort(s"strom-effect: not an effect: ${t.tpe.show}", t.pos))

  // ---- parsing --------------------------------------------------------------------------------

  private def strip(t: Term): Term = t match
    case Inlined(_, Nil, e) => strip(e)
    case Typed(e, _)        => strip(e)
    case Block(Nil, e)      => strip(e)
    case _                  => t

  /** `qual.m[..](args)..`: the selected method and its explicit arguments (implicit / using
   *  argument lists, such as ZIO's `Trace`, are dropped). */
  private def call(t: Term): Option[(Select, List[Term])] =
    def go(t: Term, argss: List[List[Term]]): Option[(Select, List[List[Term]])] = t match
      case Apply(f, as)    => go(f, as :: argss)
      case TypeApply(f, _) => go(f, argss)
      case s: Select       => Some((s, argss))
      case _               => None
    go(t, Nil).map { (s, argss) =>
      val clauses = s.symbol.paramSymss.filterNot(_.headOption.exists(_.isTypeParam))
      val explicit = argss.zip(clauses).collect {
        case (as, ps) if !ps.headOption.exists(p => p.flags.is(Flags.Given) || p.flags.is(Flags.Implicit)) => as }
      (s, explicit.flatten)
    }

  /** The `Trace` argument of a ZIO call (its implicit parameter named `trace`). */
  private def traceArg(t: Term): Option[Term] =
    def go(t: Term, argss: List[List[Term]]): Option[(Symbol, List[List[Term]])] = t match
      case Apply(f, as)    => go(f, as :: argss)
      case TypeApply(f, _) => go(f, argss)
      case s: Select       => Some((s.symbol, argss))
      case i: Ident        => Some((i.symbol, argss))
      case _               => None
    if !isZIO then None
    else go(strip(t), Nil).flatMap { (m, argss) =>
      val clauses = m.paramSymss.filterNot(_.headOption.exists(_.isTypeParam))
      argss.zip(clauses).flatMap((as, ps) => as.zip(ps)).collectFirst {
        case (a, p) if p.name == "trace" && (p.flags.is(Flags.Given) || p.flags.is(Flags.Implicit)) && a.tpe <:< TypeRepr.of[zio.Trace] => a
      }
    }

  private def inModule(owner: Symbol): Boolean =
    owner == B.module || (B.module.typeRef.baseClasses.contains(owner) && owner.fullName.startsWith(B.cls.owner.fullName))
  private object Static:
    def unapply(t: Term): Option[(String, List[Term])] = call(t).collect {
      case (s, as) if inModule(s.symbol.owner) && B.staticOp(s.name).isDefined => (B.staticOp(s.name).get, as)
    }
  private object Inst:
    def unapply(t: Term): Option[(Term, String, List[Term])] = call(t).collect {
      case (s, as) if s.symbol.owner == B.cls && B.instOp(s.name).isDefined => (s.qualifier, B.instOp(s.name).get, as)
    }

  private def lambda(f: Term): Option[(Symbol, Term)] = strip(f) match
    case b: Block => Lambda.unapply(b).collect { case (List(p), body) if !typesMention(body, Set(p.symbol)) => (p.symbol, body) }
    case _ => None

  /** Does a type name one of the symbols (`x.type`, `x.T`)? The macro gives parameters, pattern
   *  variables and local vals new symbols, and replaces them in terms only: code whose types name
   *  them is left as the user wrote it. (Inline expansion writes such types: `xs.map(_ + _)` on a
   *  pair becomes `val t: x.type = x; ...`.) */
  private def mentions(t: TypeRepr, ss: Set[Symbol]): Boolean = t match
    case r: TermRef          => ss(r.termSymbol) || mentions(r.qualifier, ss)
    case r: TypeRef          => mentions(r.qualifier, ss)
    case AppliedType(tc, as) => mentions(tc, ss) || as.exists(mentions(_, ss))
    case AndType(a, b)       => mentions(a, ss) || mentions(b, ss)
    case OrType(a, b)        => mentions(a, ss) || mentions(b, ss)
    case ByNameType(u)       => mentions(u, ss)
    case AnnotatedType(u, _) => mentions(u, ss)
    case TypeBounds(lo, hi)  => mentions(lo, ss) || mentions(hi, ss)
    case Refinement(p, _, i) => mentions(p, ss) || mentions(i, ss)
    case _                   => false

  private def typesMention(tree: Tree, ss: Set[Symbol]): Boolean =
    var found = false
    if ss.nonEmpty then
      new TreeTraverser:
        override def traverseTree(t: Tree)(o: Symbol): Unit =
          if !found then
            t match
              case tt: TypeTree if mentions(tt.tpe, ss) => found = true
              case _ => super.traverseTree(t)(o)
      .traverseTree(tree)(Symbol.spliceOwner)
    found

  private def bindsOf(pattern: Tree): Set[Symbol] =
    var out = Set.empty[Symbol]
    new TreeTraverser:
      override def traverseTree(t: Tree)(o: Symbol): Unit =
        t match
          case b: Bind => out += b.symbol
          case _ =>
        super.traverseTree(t)(o)
    .traverseTree(pattern)(Symbol.spliceOwner)
    out

  private def refersTo(t: Tree, s: Symbol): Boolean =
    var found = false
    new TreeTraverser:
      override def traverseTree(t: Tree)(o: Symbol): Unit = t match
        case i: Ident if i.symbol == s => found = true
        case _ => super.traverseTree(t)(o)
    .traverseTree(t)(Symbol.spliceOwner)
    found

  /** ZIO.succeed / attempt take `Unsafe ?=> A`: the body, if it doesn't use the capability. */
  private def thunk(t: Term): Option[Term] =
    if !isZIO then Some(t)
    else lambda(t) match
      case Some((u, body)) => if refersTo(body, u) then None else Some(body)
      case None => Some(t)

  /** Scala 3's `ZIO.succeed` is an inline def: the macro sees its expansion,
   *  `{ val ZIO_this = ZIO; ZIO.Sync(trace, () => { val u = Unsafe; body }) }`. Gives the body. */
  private object ZSync:
    private lazy val sync = Symbol.requiredModule("zio.ZIO.Sync").moduleClass
    private def moduleVal(s: Definition): Boolean = s match
      case v: ValDef => v.rhs.exists(r => r.symbol.flags.is(Flags.Module))
      case _ => false
    def unapply(t: Term): Option[Term] =
      if !isZIO then None
      else strip(t) match
        case Inlined(_, bs, e) if bs.forall(moduleVal) => unapply(e)
        case s => call(s).collect { case (sel, List(_, f)) if sel.name == "apply" && sel.symbol.owner == sync => f }
          .flatMap(f => strip(f) match
            case b: Block => Lambda.unapply(b).collect {
              case (Nil, Block(List(u: ValDef), body)) if moduleVal(u) && !refersTo(body, u.symbol) => body
              case (Nil, body) => body
            }
            case _ => None)

  private def isRef(t: Term, x: Symbol): Boolean = strip(t) match
    case i: Ident => i.symbol == x
    case _ => false

  /** A local def we compile: one parameter list, returns an effect. */
  private def loopable(d: DefDef): Boolean =
    d.rhs.isDefined && elem(d.returnTpt.tpe).isDefined && (d.paramss match
      case List(c: TermParamClause) =>
        val ps = c.params.map(_.symbol).toSet
        !c.isGiven && !c.isImplicit && !mentions(d.returnTpt.tpe, ps) && !typesMention(d.rhs.get, ps)
      case _ => false)

  private def valsOf(stats: List[Statement]): Set[Symbol] = stats.collect { case v: ValDef => v.symbol }.toSet

  private def supported(s: Statement): Boolean = s match
    case _: ValDef | _: Term | _: Import => true
    case d: DefDef => loopable(d)
    case _ => false

  private def fresh(name: String, tp: TypeRepr) =
    Symbol.newVal(Symbol.spliceOwner, name, tp, Flags.EmptyFlags, Symbol.noSymbol)

  private def parse(t0: Term)(using defs: Set[Symbol]): IR =
    val t = strip(t0)
    val ir = parse1(t0, t)
    if !traces.containsKey(ir) then traceArg(t).foreach(traces.put(ir, _))
    ir

  private def parse1(t0: Term, t: Term)(using defs: Set[Symbol]): IR =
    val tp = elemOf(t0) // before strip: an ascription can widen the type (`ZIO.fail(e): ZIO[Any, E, Int]`)
    t match
      case ZSync(body) => IR.Delay(body, tp)
      case Inlined(_, bs, _) if typesMention(t, valsOf(bs)) => IR.Opaque(t, tp)
      case Inlined(_, bs, e) => IR.Stats(bs, parse(e), defs, tp) // bindings are vals
      case Block(stats, e) if !stats.forall(supported) || typesMention(t, valsOf(stats)) => IR.Opaque(t, tp)
      case Block(stats, e) =>
        // A def is compiled only if that saves something; otherwise it stays as written and its
        // calls are opaque (compiling it would only add glue: measured slower, see the README).
        val cand = defs ++ stats.collect { case d: DefDef if loopable(d) => d.symbol }
        val ds = defs ++ stats.collect { case d: DefDef if loopable(d) && (!gate || savings(parse(d.rhs.get)(using cand)) > 0) => d.symbol }
        stats.foreach { case d: DefDef if ds(d.symbol) => defSrc(d.symbol) = (d, ds); case _ => }
        IR.Stats(stats, parse(e)(using ds), ds, tp)
      case If(c, a, b)  => IR.Cond(c, parse(a), parse(b), t, tp)
      case Match(_, cs) if cs.exists(c => typesMention(c, bindsOf(c.pattern))) => IR.Opaque(t, tp)
      case Match(s, cs) => IR.Switch(s, cs.map(c => (c.pattern, c.guard, parse(c.rhs))), t, tp)
      case Static("pure", List(v))  => IR.Pure(v, tp)
      case Static("unit", Nil)      => IR.Pure('{ () }.asTerm, tp)
      case Static("delay", List(th)) if thunk(th).isDefined => IR.Delay(thunk(th).get, tp)
      case Static("attemptDelay", List(th)) if thunk(th).isDefined => IR.Delay(B.attemptThunk(thunk(th).get, tp), tp)
      case Static("raise", List(e)) => IR.Raise(e, B.strict, tp)
      case Static("fail", List(e))  => IR.Raise(B.failWith(e), false, tp)
      case Static("defer", List(th)) => IR.Lazy(parse(th), tp)
      case Inst(r, "flatMap", List(f)) => lambda(f) match
        case Some((x, b)) => IR.Bind(parse(r), Some(x), parse(b), false, tp)
        case None         => IR.BindF(parse(r), f, tp)
      case Inst(r, "map", List(f)) => lambda(f) match
        case Some((x, b)) if isRef(b, x) => parse(r) // `yield r`: keeps a recursive call in tail position
        case Some((x, b)) => IR.Map(parse(r), Some(x), b, false, tp)
        case None         => IR.MapF(parse(r), f, tp)
      case Inst(r, op @ ("then" | "thenLazy"), List(b)) => IR.Bind(parse(r), None, parse(b), op == "then", tp)
      case Inst(r, op @ ("before" | "beforeLazy"), List(b)) =>
        val x = fresh("a", tp)
        val pb = parse(b)
        IR.Bind(parse(r), Some(x), IR.Map(pb, None, Ref(x), false, tp), op == "before", tp)
      case Inst(r, "as", List(v)) => IR.Map(parse(r), None, v, B.strict, tp)
      case Inst(r, "void", Nil)   => IR.Map(parse(r), None, '{ () }.asTerm, false, tp)
      case Inst(r, "attempt", Nil) => IR.Attempt(parse(r), fxOf(r), tp)
      case Inst(r, "handle", List(f)) => lambda(f) match
        case Some((x, b)) => IR.Handle(parse(r), x, parse(b), fxOf(r), tp)
        case None =>
          val x = fresh("e", B.errType(fxOf(r)))
          IR.Handle(parse(r), x, IR.Opaque(Select.overloaded(f, "apply", Nil, List(Ref(x))), tp), fxOf(r), tp)
      case Apply(fn: Ident, args) if defs(fn.symbol) =>
        val call = IR.Call(fn.symbol, args, None, tp)
        callPos.put(call, t.pos)
        call
      case _ => IR.Opaque(t, tp)

  /** How many effect nodes fusing removes: a bind or handler around synchronous code, a `map`
   *  (merged into what follows), what the effects given to opaque calls save, and a compiled def.
   *  An opaque call followed by a bind saves nothing: the fused code is the same `flatMap`. */
  private def savings(ir: IR)(using defs: Set[Symbol]): Int =
    // A step that ends in plain code (a map, a bind whose body is synchronous) merges into the
    // continuation that follows it: `cede.flatMap(_ => pure(i + 1)).flatMap(loop)` is one flatMap.
    def endsSync(io: IR): Boolean = io match
      case _: IR.Map | _: IR.MapF       => true
      case IR.Bind(_, _, b, _, _)       => isSync(b) || endsSync(b)
      case IR.Stats(_, e, _, _)         => endsSync(e)
      case _                            => false
    def mapped(io: IR) = if endsSync(io) then 1 else 0
    ir match
      case _: IR.Pure | _: IR.Delay | _: IR.Raise | _: IR.Call => 0
      case IR.Opaque(t, _)               => argSavings(t)
      case IR.Bind(io, _, b, _, _)       => (if isSync(io) then 1 else mapped(io)) + savings(io) + savings(b)
      case IR.BindF(io, _, _)            => (if isSync(io) then 1 else mapped(io)) + savings(io)
      case IR.Map(io, _, _, _, _)        => (if isSync(io) then 1 else 0) + savings(io)
      case IR.MapF(io, _, _)             => (if isSync(io) then 1 else 0) + savings(io)
      case IR.Stats(ss, e, ds, _) =>
        ss.map {
          case d: DefDef if ds(d.symbol) && !defs(d.symbol) => 1
          case v: ValDef if v.rhs.exists(r => elem(r.tpe).isDefined) => savings(parse(v.rhs.get)(using ds))(using ds)
          case _ => 0
        }.sum + savings(e)(using ds)
      case IR.Cond(_, a, b, _, _)        => savings(a) + savings(b)
      case IR.Switch(_, cs, _, _)        => cs.map(c => savings(c._3)).sum
      case IR.Handle(io, _, h, _, _)     => (if isSync(io) then 1 else 0) + savings(io) + savings(h)
      case IR.Attempt(io, _, _)          => (if isSync(io) then 1 else 0) + savings(io)
      case IR.Lazy(io, _)                => savings(io)
      case IR.Chosen(_, cs, _)           => cs.map(savings).sum

  /** What compiling the effects given to an opaque call saves (see [[fuseArgs]]). */
  private def argSavings(t: Term)(using defs: Set[Symbol]): Int = t match
    case Apply(fun, as) => argSavings(fun) + as.map(argSaving).sum
    case TypeApply(fun, _) => argSavings(fun)
    case Select(q, _) if elem(q.tpe).isDefined => argSaving(q)
    case Typed(e, _) => argSavings(e)
    case _ => 0

  private def argSaving(a: Term)(using defs: Set[Symbol]): Int = a match
    case NamedArg(_, e) => argSaving(e)
    case Block(List(d: DefDef), _: Closure) if d.symbol.isAnonymousFunction && d.rhs.exists(r => elem(r.tpe).isDefined) =>
      savings(parse(d.rhs.get))
    case e if elem(e.tpe).isDefined => savings(parse(e))
    case _ => 0

  private val syncCache = new java.util.IdentityHashMap[IR, java.lang.Boolean]

  /** Can the node be compiled to plain code that gives its value ([[direct]])? */
  private def isSync(ir: IR): Boolean =
    val known = syncCache.get(ir)
    if known ne null then known
    else
      val r = ir match
        case _: IR.Pure | _: IR.Delay | _: IR.Raise          => true
        case _: IR.Opaque | _: IR.Call | _: IR.BindF         => false
        case IR.Bind(io, _, b, strict, _)                    => isSync(io) && (if strict then isSync(b) else syncRegion(b))
        case IR.Map(io, _, _, _, _)                          => isSync(io)
        case IR.MapF(io, _, _)                               => isSync(io)
        case IR.Stats(_, e, _, _)                            => isSync(e)
        case IR.Cond(_, a, b, _, _)                          => syncRegion(a) && syncRegion(b)
        case IR.Switch(_, cs, _, _)                          => cs.forall(c => syncRegion(c._3))
        case IR.Handle(io, _, h, _, _)                       => isSync(io) && syncRegion(h)
        case IR.Attempt(io, _, _)                            => isSync(io)
        case IR.Lazy(io, _)                                  => syncRegion(io)
        case IR.Chosen(_, cs, _)                             => cs.forall(isSync)
      syncCache.put(ir, r)
      r

  /** A region that is synchronous and stays so when [[Hoist]] rewrites it (a part it hands back
   *  to the user's code becomes an opaque effect). */
  private def syncRegion(ir: IR): Boolean = isSync(ir) && !(order(ir, ran = false).need && bails(ir, inBranch = false))

  /** Would [[Hoist]] hand a part of this region back to the user's code? */
  private def bails(ir: IR, inBranch: Boolean): Boolean = ir match
    case IR.Stats(ss, e, _, _)        =>
      val blocked = ss.exists {
        case _: Import | _: Term => false
        case v: ValDef           => v.symbol.flags.is(Flags.Mutable) || v.symbol.flags.is(Flags.Lazy)
        case _                   => true
      }
      (inBranch && blocked) || bails(e, inBranch)
    case IR.Cond(_, a, b, _, _)       => bails(a, true) || bails(b, true)
    case IR.Switch(_, cs, _, _)       => cs.exists(c => bails(c._3, true))
    case IR.Bind(io, _, b, strict, _) => bails(io, inBranch) || (strict && bails(b, inBranch))
    case IR.BindF(io, _, _)           => bails(io, inBranch)
    case IR.Map(io, _, _, _, _)       => bails(io, inBranch)
    case IR.MapF(io, _, _)            => bails(io, inBranch)
    case IR.Handle(io, _, _, _, _)    => bails(io, inBranch)
    case IR.Attempt(io, _, _)         => bails(io, inBranch)
    case _                            => false

  // ---- when things are built ------------------------------------------------------------------

  private val primitives = Set("Int", "Long", "Short", "Byte", "Char", "Boolean", "Double", "Float", "Unit").map("scala." + _)
  private def primitive(t: TypeRepr): Boolean = primitives(t.widen.dealias.typeSymbol.fullName)

  /** A val (not a var, lazy val, def or by-name parameter): reading it has no effect and gives the
   *  same value whenever it is read. */
  private def stable(s: Symbol): Boolean =
    s.flags.is(Flags.Module) || s.isBind || (s.isValDef && !s.flags.is(Flags.Mutable) && !s.flags.is(Flags.Lazy) && {
      try s.tree match
        case v: ValDef => v.tpt.tpe match { case ByNameType(_) => false; case _ => true }
        case _ => false
      catch case _: Exception => false
    })

  /** Can `t` be evaluated at any time, any number of times, with the same result and no effect?
   *  Literals, vals, closures, and arithmetic on them. Anything else is evaluated exactly when
   *  the plain code evaluates it. */
  private def pureT(t: Term): Boolean = strip(t) match
    case _: Literal | _: This => true
    case i: Ident => stable(i.symbol)
    case s @ Select(q, _) =>
      if s.symbol.isValDef then stable(s.symbol) && pureT(q)
      else primitive(q.tpe) && s.symbol.paramSymss.isEmpty && pureT(q) // i.toLong, -i, !b
    case Apply(Select(q, op), List(a)) if primitive(q.tpe) && primitive(a.tpe) =>
      pureT(q) && pureT(a) && ((op != "/" && op != "%") || (strip(a) match
        case Literal(c) => c.value != 0 // a division that cannot throw
        case _ => false))
    case b: Block => Lambda.unapply(b).isDefined
    case _ => false

  private def stmtBuilt(s: Statement): Boolean = s match
    case v: ValDef => v.rhs.exists(r => !pureT(r))
    case _: Term   => true
    case _         => false

  /** need: something of the region is built after one of its effects has run, in the code we would
   *  emit, or under a handler that must not see it. ran: an effect may have run. built: the region
   *  builds something that is not pure. */
  private final case class Order(need: Boolean, ran: Boolean, built: Boolean)

  private def order(ir: IR, ran: Boolean): Order = ir match
    case IR.Pure(v, _)       => val b = !pureT(v); Order(ran && b, ran, b)
    case _: IR.Delay         => Order(false, true, false)
    case IR.Raise(e, st, _)  => val b = st && !pureT(e); Order(ran && b, true, b)
    case IR.Opaque(t, _)     => val b = !pureT(t); Order(ran && b, true, b)
    // Calling a split def builds the start of its body (see [[isSplit]]).
    case IR.Call(f, as, plan, _) => val b = as.exists(a => !pureT(a)) || (plan.isEmpty && isSplit(f)); Order(ran && b, true, b)
    case _: IR.Lazy          => Order(false, true, false)
    case IR.Bind(io, _, body, strict, _) =>
      val a = order(io, ran)
      if !strict then Order(a.need, true, a.built)
      else { val x = order(body, a.ran); Order(a.need || x.need, x.ran, a.built || x.built) }
    case IR.BindF(io, f, _)  => val a = order(io, ran); val b = !pureT(f); Order(a.need || (a.ran && b), true, a.built || b)
    case IR.Map(io, _, v, strict, _) =>
      val a = order(io, ran); val b = strict && !pureT(v); Order(a.need || (a.ran && b), a.ran, a.built || b)
    case IR.MapF(io, f, _)   => val a = order(io, ran); val b = !pureT(f); Order(a.need || (a.ran && b), a.ran, a.built || b)
    case IR.Stats(ss, e, _, _) =>
      val b = ss.exists(stmtBuilt); val a = order(e, ran); Order((ran && b) || a.need, a.ran, b || a.built)
    case IR.Cond(c, x, y, _, _) =>
      // With nothing run before, each branch is a region of its own (built when it is chosen).
      val ox = order(x, ran); val oy = order(y, ran); val b = !pureT(c) || ox.built || oy.built
      Order(ran && b, ox.ran || oy.ran, b)
    case IR.Switch(sc, cs, _, _) =>
      val os = cs.map(c => order(c._3, ran))
      val b = !pureT(sc) || cs.exists(_._2.exists(g => !pureT(g))) || os.exists(_.built)
      Order(ran && b, os.exists(_.ran), b)
    // What the handled effect builds is built outside the handler: an exception thrown while
    // building is not the handler's to catch.
    case IR.Handle(io, _, _, _, _) => val a = order(io, ran); Order(a.need || a.built, true, a.built)
    case IR.Attempt(io, _, _)      => val a = order(io, ran); Order(a.need || a.built, true, a.built)
    case IR.Chosen(_, cs, _)       => Order(false, ran || cs.exists(c => order(c, ran).ran), false) // built already

  // ---- emission -------------------------------------------------------------------------------

  /** A compiled local def's loop state: the parameters as vars, and the iteration counter.
   *  orig: the user's def symbol; run: the compiled method. */
  private final case class Loop(orig: Symbol, run: Symbol, vars: List[Symbol], n: Symbol)

  /** owner: the symbol owning new definitions. fx, r: the effect the code returns (fx with value r).
   *  async: inside a generated lambda (so a tail call must call the def, not jump).
   *  inDef: inside a compiled def (non-tail calls go through defer, for stack safety).
   *  sub: user symbols replaced by ours. runs: user defs to their compiled methods.
   *  heads: split defs to the method that builds the start of their body. */
  private final case class Ctx(owner: Symbol, fx: Fx, r: TypeRepr, loop: Option[Loop], async: Boolean,
                               inDef: Boolean, sub: Map[Symbol, Symbol], runs: Map[Symbol, Symbol] = Map.empty,
                               heads: Map[Symbol, Symbol] = Map.empty):
    def bind(x: Option[Symbol], v: Term): Ctx = x.fold(this)(s => copy(sub = sub + (s -> v.symbol)))
    /** A region compiled on its own (inside handle / attempt). */
    def region(fx: Fx, r: TypeRepr): Ctx = copy(fx = fx, r = r, loop = None)

  private def ioOf(a: TypeRepr)(using c: Ctx): TypeRepr = B.effect(c.fx, a)

  /** What happens to a value: return it as the result, or run more code. */
  private enum K:
    case Ret
    case Fn(f: (Term, Ctx) => Term, trace: Option[Term] = None) // trace: of the user's call (ZIO)

  private var counter = 0
  private def name(s: String) = { counter += 1; s"$s$$$counter" }

  private final class Subst(m: Map[Symbol, Symbol], defs: Symbol => Boolean) extends TreeMap:
    override def transformTerm(t: Term)(owner: Symbol): Term = t match
      case i: Ident if m.contains(i.symbol) =>
        if defs(i.symbol) then noteValue(i)
        Ref(m(i.symbol))
      case _ => super.transformTerm(t)(owner)

  private def user(t: Term)(using c: Ctx): Term =
    val s = if c.sub.isEmpty then t else Subst(c.sub, c.runs.contains).transformTerm(t)(c.owner)
    s.changeOwner(c.owner)

  /** An opaque effect expression, with the effects it is given compiled too: an effect-typed
   *  argument or receiver of the call, and the body of a lambda argument that returns an effect
   *  (`IO.uncancelable(poll => ..)`, `io.guarantee(fin)`, `IO.both(a, b)`, `repo.inTx(io)`). Each
   *  is a block of its own, built where the plain code builds it. */
  private def opq(t: Term)(using Ctx): Term = user(fuseArgs(t))

  private def fuseArgs(t: Term)(using c: Ctx): Term = t match
    case Apply(fun, as) =>
      val (f2, as2) = (fuseArgs(fun), as.map(fuseArg))
      if (f2 eq fun) && as2.corresponds(as)(_ eq _) then t else Apply.copy(t)(f2, as2)
    case TypeApply(fun, ts) => val f2 = fuseArgs(fun); if f2 eq fun then t else TypeApply.copy(t)(f2, ts)
    case Select(q, n) if elem(q.tpe).isDefined => val q2 = fuseArg(q); if q2 eq q then t else Select.copy(t)(q2, n)
    case Typed(e, tpt) => val e2 = fuseArgs(e); if e2 eq e then t else Typed.copy(t)(e2, tpt)
    case _ => t

  private def fuseArg(a: Term)(using c: Ctx): Term = a match
    case NamedArg(n, e) => val e2 = fuseArg(e); if e2 eq e then a else NamedArg.copy(a)(n, e2)
    case b @ Block(List(d: DefDef), cl: Closure) if d.symbol.isAnonymousFunction && d.rhs.exists(r => elem(r.tpe).isDefined) =>
      compiled(d.rhs.get, d.symbol) match
        case Some(code) => Block.copy(b)(List(DefDef.copy(d)(d.name, d.paramss, d.tpt, Some(code))), cl)
        case None => a
    case e if elem(e.tpe).isDefined => compiled(e, c.owner).getOrElse(a)
    case _ => a

  /** The effect expression compiled as a block of its own, if there is something to fuse in it. */
  private def compiled(e: Term, owner: Symbol)(using c: Ctx): Option[Term] =
    val inner = c.copy(owner = owner, loop = None, async = true, inDef = false)
    parse(e)(using c.runs.keySet) match
      case IR.Opaque(t2, _) =>
        val t3 = fuseArgs(t2)(using inner)
        if t3 eq t2 then None else Some(t3)
      case _: IR.Pure | _: IR.Delay | _: IR.Raise => None // one node: compiling would only add a defer
      case ir =>
        val (fx, a) = split(e.tpe).get
        Some(Typed(build(ir)(using inner.copy(fx = fx, r = a)), Inferred(e.tpe.widen)))

  private def raise(e: Term)(using Ctx): Term = '{ throw ${ e.asExprOf[Throwable] } }.asTerm.owned

  /** defer(boundary(body)): code that runs later, as its own boundary. */
  private def later(body: Term, a: TypeRepr)(using c: Ctx): Term = B.defer(B.boundary(body, a), a)

  private def bindVal(rhs: Term, tp: TypeRepr)(body: (Term, Ctx) => Term)(using c: Ctx): Term =
    val s = Symbol.newVal(c.owner, name("v"), tp, Flags.EmptyFlags, Symbol.noSymbol)
    Block(List(ValDef(s, Some(rhs.changeOwner(s)))), chunk(body(Ref(s), c)))

  // ---- method size ----------------------------------------------------------------------------

  /** HotSpot does not compile a method of 8000 bytes or more: it stays interpreted. Code over this
   *  many tree nodes is moved into a local def, which is a JVM method of its own. A node is about
   *  two bytes of bytecode here (MethodSizeSuite measures the result). */
  private val MaxNodes = Compiler.MaxNodes

  /** Tree nodes, counted up to `cap`. Lambdas and defs inside are methods already: one node each. */
  private def nodes(t: Tree, cap: Int): Int =
    var n = 0
    new TreeTraverser:
      override def traverseTree(t: Tree)(o: Symbol): Unit =
        if n <= cap then
          n += 1
          t match
            case _: DefDef =>
            case _ => super.traverseTree(t)(o)
    .traverseTree(t)(Symbol.spliceOwner)
    n

  private def outline(t: Term)(using c: Ctx): Term =
    val m = Symbol.newMethod(c.owner, name("part"), MethodType(Nil)(_ => Nil, _ => t.tpe.widen))
    Block(List(DefDef(m, _ => Some(t.changeOwner(m)))), Apply(Ref(m), Nil))

  private def chunk(t: Term)(using Ctx): Term = if nodes(t, MaxNodes) > MaxNodes then outline(t) else t

  /** The branches of one `if` / `match`: the largest are outlined until they fit
   *  one method together. */
  private def chunkAll(bs: List[Term])(using Ctx): List[Term] =
    val out = scala.collection.mutable.ArrayBuffer.from(bs)
    val sizes = bs.map(nodes(_, MaxNodes)).toArray
    while sizes.sum > MaxNodes && sizes.max > 40 do
      val i = sizes.indexOf(sizes.max)
      out(i) = outline(out(i))
      sizes(i) = 4
    out.toList

  private def ifT(c: Term, a: Term, b: Term)(using Ctx): Term =
    val List(x, y) = chunkAll(List(a, b)): @unchecked
    If(c, x, y)

  private def matchT(s: Term, cases: List[CaseDef])(using Ctx): Term =
    Match(s, cases.zip(chunkAll(cases.map(_.rhs))).map((cd, rhs) => CaseDef(cd.pattern, cd.guard, rhs)))

  private def lam(param: TypeRepr, res: TypeRepr)(body: (Term, Ctx) => Term)(using c: Ctx): Term =
    val mt = MethodType(List(name("x")))(_ => List(param), _ => B.effect(c.fx, res))
    Lambda(c.owner, mt, (meth, ps) =>
      given Ctx = c.copy(owner = meth, async = true, r = res)
      B.boundary(body(ps.head.asInstanceOf[Term], summon[Ctx]), res))

  private def applyF(f: Term, v: Term): Term = Select.overloaded(f, "apply", Nil, List(v))

  /** Value code (sync IR only). */
  private def direct(ir: IR)(using c: Ctx): Term = ir match
    case IR.Pure(v, _)  => user(v)
    case IR.Delay(t, _) => user(t)
    case IR.Raise(e, _, _) => raise(user(e))
    case IR.Bind(io, x, b, strict, _) => bindVal(direct(io), io.tp) { (v, c2) =>
      given Ctx = c2.bind(x, v)
      if strict then direct(b) else directR(b) }
    case IR.Map(io, x, b, _, _) => bindVal(direct(io), io.tp)((v, c2) => user(b)(using c2.bind(x, v)))
    case IR.MapF(io, f, _)    => bindVal(direct(io), io.tp)((v, c2) => applyF(user(f)(using c2), v))
    case IR.Stats(ss, e, ds, _) => stats(ss, ds)(direct(e))
    case IR.Lazy(io, _)         => directR(io)
    case IR.Cond(cnd, a, b, _, _) => ifT(user(cnd), directR(a), directR(b))
    case IR.Switch(s, cs, _, _) =>
      matchT(user(s), cs.map((p, g, b) => caseDef(p, g)((c2: Ctx) ?=> directR(b)(using c2))))
    case IR.Chosen(w, cs, _) =>
      matchT(user(w), cs.zipWithIndex.map((b, i) => CaseDef(int(i), None, direct(b))))
    case IR.Handle(io, x, h, fxIn, _) =>
      Try(direct(io), List(B.catchCase(fxIn)((e, c2) => directR(h)(using c2.bind(Some(x), e)))), None)
    case IR.Attempt(io, fxIn, tp) =>
      withT(B.errType(fxIn))([e] => (_: Type[e]) ?=> withT(io.tp)([a] => (_: Type[a]) ?=>
        Try('{ Right[e, a](${ direct(io).asExprOf[a] }) }.asTerm.owned,
            List(B.catchCase(fxIn)((e, c2) => '{ Left[e, a](${ e.asExprOf[e] }) }.asTerm)), None)))
    case _ => report.errorAndAbort(s"strom-effect: not synchronous: $ir")

  private def applyK(k: K, v: Term, tp: TypeRepr)(using c: Ctx): Term = k match
    case K.Ret      => B.pure(v, tp)
    case K.Fn(f, _) => bindVal(v, tp)(f)

  /** Run `io` (of value type tp), then k. */
  private def suspend(io: Term, tp: TypeRepr, k: K)(using c: Ctx): Term = k match
    case K.Ret   => io
    case K.Fn(f, tr) => B.flatMap(io, tp, lam(tp, c.r)(f), tr)

  private def nullIO: Term = Literal(NullConstant())

  /** A continuation used by several branches becomes a local def (a join point). */
  private def join(k: K, tp: TypeRepr)(body: K => Term)(using c: Ctx): Term = k match
    case K.Ret => body(K.Ret)
    case K.Fn(f, tr) =>
      val j = Symbol.newMethod(c.owner, name("join"), MethodType(List("v"))(_ => List(tp), _ => ioOf(c.r)))
      val dd = DefDef(j, { case List(List(v: Term)) => Some(f(v, c.copy(owner = j)).changeOwner(j)); case _ => None })
      // In a loop, j may return null ("continue": it already assigned the loop vars). From a lambda
      // (after an opaque step) there is no while loop to continue, so start a new run instead.
      def call(v: Term, c2: Ctx): Term = (c.loop, c.async, c2.async) match
        case (Some(l), false, true) =>
          val r = Symbol.newVal(c2.owner, name("r"), ioOf(c.r), Flags.EmptyFlags, Symbol.noSymbol)
          val rE = Ref(r).asExprOf[AnyRef]
          Block(List(ValDef(r, Some(Apply(Ref(j), List(v))))),
                If('{ $rE eq null }.asTerm, Apply(Ref(l.run), l.vars.map(Ref(_))), Ref(r)))
        case _ => Apply(Ref(j), List(v))
      Block(List(dd), body(K.Fn(call, tr)))

  /** Code returning the effect (or null inside a loop: "continue with the updated vars"). */
  private val stepCount = new java.util.IdentityHashMap[IR, Integer]

  /** The steps the plain code runs one after the other (roughly: the runtime's loop iterations). */
  private def steps(ir: IR): Int =
    val known = stepCount.get(ir)
    if known ne null then known
    else
      val n = ir match
        case _: IR.Pure | _: IR.Delay | _: IR.Raise | _: IR.Opaque | _: IR.Call => 1
        case IR.Bind(io, _, b, _, _)   => steps(io) + steps(b)
        case IR.BindF(io, _, _)        => steps(io) + 1
        case IR.Map(io, _, _, _, _)    => steps(io) + 1
        case IR.MapF(io, _, _)         => steps(io) + 1
        case IR.Stats(_, e, _, _)      => steps(e)
        case IR.Cond(_, a, b, _, _)    => steps(a) max steps(b)
        case IR.Switch(_, cs, _, _)    => cs.map(c => steps(c._3)).maxOption.getOrElse(1)
        case IR.Handle(io, _, h, _, _) => steps(io) + steps(h)
        case IR.Attempt(io, _, _)      => steps(io) + 1
        case IR.Lazy(io, _)            => steps(io)
        case IR.Chosen(_, cs, _)       => cs.map(steps).maxOption.getOrElse(1)
      stepCount.put(ir, n)
      n

  /** In a synchronous stretch longer than the yield interval: does a multiple of the interval fall
   *  between `before` and the `after` steps that follow? Then the code cedes there, as a compiled
   *  loop does every so many iterations (the plain code is checked for cancelation and yields as it
   *  goes; one long piece of direct code would not be). */
  private def pause(before: Int, after: Int): Boolean =
    if before <= every then (after - 1) / every != (after + before - 1) / every // the last piece is a whole interval
    else before / every != (before + after) / every

  private def cedeThen(rest: Ctx ?=> Term)(using Ctx): Term =
    suspend(B.cede, TypeRepr.of[Unit], K.Fn((_, c2) => rest(using c2)))

  private def cps(ir: IR, k: K)(using c: Ctx): Term =
    val tr = traceOf(ir)
    val sync = isSync(ir)
    if sync && steps(ir) <= every then
      k match
        case K.Ret      => B.pure(direct(ir), ir.tp)
        case K.Fn(f, _) => bindVal(direct(ir), ir.tp)(f)
    else ir match
      case IR.Opaque(t, tp)          => suspend(opq(t), tp, k)
      case IR.Call(sym, args, plan, tp) => callDef(sym, args, plan, tp, k)
      case IR.Bind(io, x, b, strict, _) => cps(io, K.Fn({ (v, c2) =>
        given Ctx = c2.bind(x, v)
        def rest(using Ctx): Term = if strict then cps(b, k) else cpsR(b, k)
        if sync && pause(steps(io), steps(b)) then cedeThen(rest) else rest }, tr))
      case IR.BindF(io, f, tp)       => cps(io, K.Fn((v, c2) => suspend(applyF(user(f)(using c2), v), tp, k)(using c2), tr))
      case IR.Map(io, x, b, _, tp)   => cps(io, K.Fn({ (v, c2) =>
        given Ctx = c2.bind(x, v)
        def rest(using Ctx): Term = applyK(k, user(b), tp)
        if sync && steps(io) > every && pause(steps(io), 1) then cedeThen(rest) else rest }, tr))
      case IR.MapF(io, f, tp)        => cps(io, K.Fn((v, c2) => { given Ctx = c2; applyK(k, applyF(user(f), v), tp) }, tr))
      case IR.Stats(ss, e, ds, _)    => stats(ss, ds)(cps(e, k))
      case IR.Lazy(io, _)            => cpsR(io, k)
      case IR.Cond(cnd, a, b, _, tp) => join(k, tp)(k2 => ifT(user(cnd), cpsR(a, k2), cpsR(b, k2)))
      case IR.Switch(s, cs, _, tp) =>
        join(k, tp)(k2 => matchT(user(s), cs.map((p, g, b) => caseDef(p, g)((c2: Ctx) ?=> cpsR(b, k2)(using c2)))))
      case IR.Chosen(w, cs, tp) =>
        join(k, tp)(k2 => matchT(user(w), cs.zipWithIndex.map((b, i) => CaseDef(int(i), None, cps(b, k2)))))
      case IR.Handle(io, x, h, fxIn, tp) if isSync(io) && steps(io) <= every =>
        // The handled part is synchronous, the handler is not: try/catch around the part, then the
        // handler as code of its own after the try (it is not its own handler), so a loop that
        // recovers by calling itself stays a jump.
        withT(B.errType(fxIn))([e] => (_: Type[e]) ?=>
          val failed = Symbol.newVal(c.owner, name("failed"), TypeRepr.of[Boolean], Flags.Mutable, Symbol.noSymbol)
          val err = Symbol.newVal(c.owner, name("err"), TypeRepr.of[e], Flags.Mutable, Symbol.noSymbol)
          // A part of type Nothing never gives a value; its val is an Any (null.asInstanceOf[Nothing] throws).
          val nothing = io.tp =:= TypeRepr.of[Nothing]
          val vT = if nothing then TypeRepr.of[Any] else io.tp
          val v = Symbol.newVal(c.owner, name("v"), vT, Flags.EmptyFlags, Symbol.noSymbol)
          val caught = B.catchCase(fxIn)((ex, _) =>
            Block(List(Assign(Ref(failed), Literal(BooleanConstant(true))), Assign(Ref(err), ex)), zero(vT)))
          val tried = Try(direct(io), List(caught), None)
          Block(List(ValDef(failed, Some(Literal(BooleanConstant(false)))), ValDef(err, Some(zero(TypeRepr.of[e]))),
                     ValDef(v, Some(tried.changeOwner(v)))),
            join(k, tp)(k2 => ifT(Ref(failed),
              bindVal(Ref(err), TypeRepr.of[e])((ev, c2) => cpsR(h, k2)(using c2.bind(Some(x), ev))),
              applyK(k2, if nothing then castTo(Ref(v), io.tp) else Ref(v), tp)))))
      case IR.Handle(io, x, h, fxIn, tp) =>
        // Errors from opaque parts arrive in the error channel: keep a real handler.
        val rin = c.region(fxIn, io.tp)
        val inner = B.boundary(cps(io, K.Ret)(using rin), io.tp)(using rin)
        val rout = c.region(c.fx, tp)
        val h1 = lam(B.errType(fxIn), tp)((e, c2) => cpsR(h, K.Ret)(using c2.region(c.fx, tp).bind(Some(x), e)))(using rout)
        suspend(B.handled(inner, io.tp, fxIn, h1, tp, tr), tp, k) // the outer fx: the handler's error type
      case IR.Attempt(io, fxIn, tp) =>
        val rin = c.region(fxIn, io.tp)
        val inner = B.boundary(cps(io, K.Ret)(using rin), io.tp)(using rin)
        suspend(B.attempted(inner, io.tp, fxIn, tr)(using rin), tp, k)
      case _ => report.errorAndAbort(s"strom-effect: unexpected $ir")

  private def callDef(sym0: Symbol, args0: List[Term], plan0: Option[Term], tp: TypeRepr, k: K)(using c: Ctx): Term =
    val run = c.runs(sym0)
    /** The arguments as vals; for a split def also its plan: the one built ahead, or its head
     *  called here (the call is where the plain code builds the start of the body). */
    def withArgs(body: (List[Term], Ctx) => Term): Term =
      def go(as: List[Term], vs: List[Term])(using c2: Ctx): Term = as match
        case a :: rest => bindVal(a, a.tpe.widen)((v, c3) => go(rest, vs :+ v)(using c3))
        case Nil => c.heads.get(sym0) match
          case None => body(vs, c2)
          case Some(h) =>
            val plan = plan0.map(p => user(p)(using c2)).getOrElse(Apply(Ref(h), vs))
            bindVal(plan, TypeRepr.of[AnyRef])((pl, c3) => body(vs :+ pl, c3))
      go(args0.map(user), Nil)
    c.loop match
      case Some(l) if l.orig == sym0 && k == K.Ret && !c.async =>
        // Jump: evaluate the arguments, assign the vars, continue the while loop (or cede).
        withArgs { (vs, _) =>
          val n = Ref(l.n).asExprOf[Int]
          val assign = l.vars.zip(vs).map((x, v) => Assign(Ref(x), v))
          val cede = B.flatMap(B.cede, TypeRepr.of[Unit], lam(TypeRepr.of[Unit], c.r)((_, _) => Apply(Ref(run), vs)), None)
          val cont = Block(assign, nullIO)
          Block(List(Assign(Ref(l.n), '{ $n + 1 }.asTerm)),
            If('{ $n >= ${ Expr(every) } }.asTerm, cede, cont))
        }
      case Some(l) if l.orig == sym0 && k == K.Ret => withArgs((vs, _) => Apply(Ref(run), vs)) // resumed in a lambda: a new run
      case _ =>
        withArgs { (vs, c2) =>
          val call = Apply(Ref(run), vs)
          suspend(if c.inDef then B.defer(call, tp) else call, tp, k)(using c2)
        }

  /** A case with fresh pattern binds (owned here). */
  private def caseDef(p: Tree, g: Option[Term])(body: Ctx ?=> Term)(using c: Ctx): CaseDef =
    val m = scala.collection.mutable.LinkedHashMap.empty[Symbol, Symbol]
    object Binds extends TreeMap:
      override def transformTree(t: Tree)(owner: Symbol): Tree = t match
        case b: Bind =>
          val s = Symbol.newBind(c.owner, b.name, Flags.EmptyFlags, b.symbol.termRef.widen)
          m(b.symbol) = s
          Bind(s, transformTree(b.pattern)(owner))
        case _ => super.transformTree(t)(owner)
    val pat = Binds.transformTree(p)(c.owner)
    val cb = c.copy(sub = c.sub ++ m)
    val guard = g.map(user(_)(using cb))
    CaseDef(pat, guard, body(using cb))

  /** User statements followed by code. Their vals and defs get fresh symbols owned by the current
   *  owner (a definition can't be re-owned apart from its uses); references are substituted. */
  private def stats(ss: List[Statement], defs: Set[Symbol])(rest: Ctx ?=> Term)(using c0: Ctx): Term =
    val (out, c) = statsDefs(ss, defs)
    Block(out, rest(using c))

  private def statsDefs(ss: List[Statement], defs: Set[Symbol])(using c0: Ctx): (List[Statement], Ctx) =
    val fresh: List[(Statement, Option[Symbol])] = ss.map {
      case v: ValDef =>
        val f = Seq(Flags.Mutable, Flags.Lazy).filter(v.symbol.flags.is).foldLeft(Flags.EmptyFlags)(_ | _)
        (v, Some(Symbol.newVal(c0.owner, v.name, v.tpt.tpe, f, Symbol.noSymbol)))
      case d: DefDef => (d, Some(Symbol.newMethod(c0.owner, d.name, methodType(d))))
      case s => (s, None)
    }
    // Each def becomes `f$run` (runs now; what our code calls) and `f` = defer(f$run(..)), for
    // user code that uses f as a value.
    val anyRef = TypeRepr.of[AnyRef]
    val splits = fresh.collect { case (d: DefDef, _) if defs(d.symbol) && isSplit(d.symbol) => d.symbol }.toSet
    val runs = fresh.collect { case (d: DefDef, _) if defs(d.symbol) =>
      d.symbol -> Symbol.newMethod(c0.owner, name(d.name + "$run"), methodType(d, plan = splits(d.symbol))) }.toMap
    val heads = fresh.collect { case (d: DefDef, _) if splits(d.symbol) =>
      d.symbol -> Symbol.newMethod(c0.owner, name(d.name + "$head"), methodType(d, result = Some(anyRef))) }.toMap
    val c = c0.copy(sub = c0.sub ++ fresh.collect { case (d: Definition, Some(n)) => d.symbol -> n },
                    runs = c0.runs ++ runs, heads = c0.heads ++ heads)
    val out = fresh.flatMap {
      case (d: DefDef, Some(n)) if !defs(d.symbol) => // not compiled (nothing to save): as written
        val params = d.termParamss.head.params
        List(DefDef(n, { case List(ps) =>
          Some(user(d.rhs.get)(using c.copy(owner = n, sub = c.sub ++ params.map(_.symbol).zip(ps.map(_.symbol)))))
          case _ => None }))
      case (v: ValDef, Some(n)) => List(ValDef(n, v.rhs.map(r => user(fuseArg(r)(using c))(using c).changeOwner(n))))
      case (d: DefDef, Some(n)) =>
        val (fx, x) = split(d.returnTpt.tpe).get
        // The def the user's code sees: it builds the start of the body when it is called (a split
        // def), and what it returns runs the rest, any number of times.
        val lzy = DefDef(n, { case List(ps) =>
          given Ctx = c.copy(owner = n, fx = fx)
          val args = ps.map(p => Ref(p.symbol))
          Some(heads.get(d.symbol) match
            case Some(h) => bindVal(Apply(Ref(h), args), anyRef)((pl, _) => B.defer(Apply(Ref(runs(d.symbol)), args :+ pl), x))
            case None    => B.defer(Apply(Ref(runs(d.symbol)), args), x))
          case _ => None })
        compileDef(d, runs(d.symbol), defs, heads.get(d.symbol))(using c) :+ lzy
      case (t: Term, _)         => List(user(t)(using c))
      case (s, _)               => List(s)
    }
    (out, c)

  // ---- building in the plain code's order -----------------------------------------------------

  private object Bail extends scala.util.control.ControlThrowable

  /** Rewrites a region so that everything the plain code evaluates while building it is evaluated
   *  first, in source order, into vals: opaque effect expressions, by-value arguments, statements.
   *  The rewritten region only runs. Inside the branches of an `if` or `match` the vals are vars
   *  assigned in the chosen branch; a `match` is decided here (its case and pattern variables kept
   *  in vars) and only replayed later; a val in a branch becomes a var too. What cannot be done that
   *  way (a local def, a var or a lazy val in a branch) is left to the user's own code: the whole
   *  expression becomes one opaque value. */
  private final class Hoist(var ctx: Ctx, once: Boolean = false): // once: for a block that is built once and may run many times
    var decls = List.empty[Statement] // reversed
    var stmts = List.empty[Statement] // reversed
    private var inBranch = 0
    def statements: List[Statement] = decls.reverse ++ stmts.reverse

    private def value(t: Term, effect: Boolean = false): Term =
      if pureT(t) then t
      else
        given Ctx = ctx
        term(if effect then opq(t) else user(t), t.tpe.widen)

    /** A term of ours (already in its final form) evaluated here, once. */
    private def term(rhs: Term, tp: TypeRepr): Term =
      if inBranch == 0 then
        val s = Symbol.newVal(ctx.owner, name("b"), tp, Flags.EmptyFlags, Symbol.noSymbol)
        stmts ::= ValDef(s, Some(rhs.changeOwner(s)))
        Ref(s)
      else
        val s = Symbol.newVal(ctx.owner, name("b"), tp, Flags.Mutable, Symbol.noSymbol)
        decls ::= ValDef(s, Some(zero(tp)))
        stmts ::= Assign(Ref(s), rhs)
        Ref(s)

    private def branch(ir: IR): (List[Statement], IR) =
      val saved = stmts
      stmts = Nil
      inBranch += 1
      try { val r = go(ir); (stmts.reverse, r) }
      finally { stmts = saved; inBranch -= 1 }

    def go(ir: IR): IR = keepTrace(ir, ir match
      case IR.Pure(v, tp)        => IR.Pure(value(v), tp)
      case IR.Raise(e, true, tp) => IR.Raise(value(e), true, tp)
      case IR.Opaque(t, tp)      => IR.Opaque(value(t, effect = true), tp)
      case IR.Call(f, as, None, tp) if ctx.heads.contains(f) =>
        // The plain code calls f here: that builds the start of f's body. The arguments are used
        // twice (by the head now, by the call later), so all of them become vals.
        val vs = as.map(a => term(user(a)(using ctx), a.tpe.widen))
        val plan = term(Apply(Ref(ctx.heads(f)), vs), TypeRepr.of[AnyRef])
        IR.Call(f, vs, Some(plan), tp)
      case IR.Call(f, as, plan, tp) =>
        if once && plan.isEmpty then Option(callPos.get(ir)).foreach(noteLate(f, _))
        IR.Call(f, as.map(a => value(a)), plan, tp)
      case _: IR.Delay | _: IR.Raise | _: IR.Lazy => ir
      case IR.Bind(io, x, b, strict, tp) => val i = go(io); IR.Bind(i, x, if strict then go(b) else b, strict, tp)
      case IR.BindF(io, f, tp)   => val i = go(io); IR.BindF(i, value(f), tp)
      case IR.Map(io, x, v, strict, tp) => val i = go(io); IR.Map(i, x, if strict then value(v) else v, strict, tp)
      case IR.MapF(io, f, tp)    => val i = go(io); IR.MapF(i, value(f), tp)
      case IR.Stats(ss, e, ds, _) if inBranch > 0 =>
        // In a branch: a val becomes a var assigned here, another statement runs here. A local
        // def, a var or a lazy val cannot be built ahead.
        ss.foreach {
          case _: Import =>
          case v: ValDef if !v.symbol.flags.is(Flags.Mutable) && !v.symbol.flags.is(Flags.Lazy) && v.rhs.isDefined =>
            given Ctx = ctx
            val tp = v.tpt.tpe
            val s = Symbol.newVal(ctx.owner, name(v.name), tp, Flags.Mutable, Symbol.noSymbol)
            decls ::= ValDef(s, Some(zero(tp)))
            stmts ::= Assign(Ref(s), user(fuseArg(v.rhs.get)))
            ctx = ctx.copy(sub = ctx.sub + (v.symbol -> s))
          case t: Term => stmts ::= user(t)(using ctx)
          case _ => throw Bail
        }
        go(e)
      case IR.Stats(ss, e, ds, _) =>
        val (out, c2) = statsDefs(ss, ds)(using ctx)
        stmts = out.reverse ++ stmts
        ctx = c2
        go(e)
      case IR.Cond(c, a, b, src, tp) =>
        val (d0, s0, x0) = (decls, stmts, ctx)
        try
          val c2 = value(c)
          val (sa, a2) = branch(a)
          val (sb, b2) = branch(b)
          if sa.nonEmpty || sb.nonEmpty then
            val unit = Literal(UnitConstant())
            stmts ::= If(user(c2)(using ctx), Block(sa, unit), Block(sb, unit))
          IR.Cond(c2, a2, b2, src, tp)
        catch case Bail =>
          if inBranch > 0 then throw Bail
          decls = d0; stmts = s0; ctx = x0
          IR.Opaque(value(src, effect = true), tp)
      case IR.Switch(sc, cs, src, tp) =>
        // Decide the match now: which case, its pattern variables, what that case builds.
        val (d0, s0, c0) = (decls, stmts, ctx)
        try
          val which = Symbol.newVal(ctx.owner, name("case"), TypeRepr.of[Int], Flags.Mutable, Symbol.noSymbol)
          decls ::= ValDef(which, Some(int(-1)))
          val unit = Literal(UnitConstant())
          val scrutinee = user(sc)(using ctx)
          val done = cs.zipWithIndex.map { case ((p, g, b), i) =>
            var chosen: IR = null
            val cd = caseDef(p, g) { (cb: Ctx) ?=>
              // The pattern variables live on in vars: the rest of the case is emitted later.
              val kept = bindsOf(p).toList.map { ub =>
                val here = cb.sub(ub)
                val v = Symbol.newVal(ctx.owner, name(ub.name), here.termRef.widen, Flags.Mutable, Symbol.noSymbol)
                decls ::= ValDef(v, Some(zero(here.termRef.widen)))
                (ub, v, Assign(Ref(v), Ref(here)))
              }
              ctx = ctx.copy(sub = ctx.sub ++ kept.map((ub, v, _) => ub -> v))
              val (built, b2) = branch(b)
              chosen = b2
              Block(Assign(Ref(which), int(i)) :: kept.map(_._3) ::: built, unit)
            }(using ctx)
            (cd, chosen)
          }
          stmts ::= Match(scrutinee, done.map(_._1))
          IR.Chosen(Ref(which), done.map(_._2), tp)
        catch case Bail =>
          if inBranch > 0 then throw Bail
          decls = d0; stmts = s0; ctx = c0
          IR.Opaque(value(src, effect = true), tp)
      case c: IR.Chosen => c
      case IR.Handle(io, x, h, fxIn, tp) => IR.Handle(go(io), x, h, fxIn, tp)
      case IR.Attempt(io, fxIn, tp)      => IR.Attempt(go(io), fxIn, tp))

  private def bodyOf(d: DefDef, defs: Set[Symbol]): IR = bodies.getOrElseUpdate(d.symbol, parse(d.rhs.get)(using defs))

  /** Does the def's body build something that is not pure before anything of it runs? */
  private def startsBuilding(f: Symbol): Boolean =
    defSrc.get(f).exists((d, ds) => order(bodyOf(d, ds), ran = false).built)

  private val splitKnown = scala.collection.mutable.Map.empty[Symbol, Boolean]
  private val splitting = scala.collection.mutable.Set.empty[Symbol]

  /** A def whose body starts by building something is split in two: `f$head(args)` builds what the
   *  plain code builds when it calls `f(args)` and returns it (the plan); `f$run(args, plan)` runs.
   *  So `a *> f(x)` can build before `a` runs, and the effect `f(x)` is built once however often
   *  it runs. Not possible when the start of the body has a local def, a var or a lazy val (those
   *  cannot be handed from one method to the other): then the body is built when it runs, and
   *  [[noteCalls]] tells the user where that shows. */
  private def isSplit(f: Symbol): Boolean = splitKnown.getOrElse(f, {
    if splitting(f) then false // asked while deciding f itself: a call of f does not make f split
    else defSrc.get(f) match
      case None => false
      case Some((d, ds)) =>
        splitting += f
        val r =
          try { val ir = bodyOf(d, ds); order(ir, ran = false).built && !bails(ir, inBranch = false) && headable(ir) }
          finally splitting -= f
        splitKnown(f) = r
        r
  })

  /** Can everything the start of the body builds be handed over as values? */
  private def headable(ir: IR): Boolean = ir match
    case IR.Stats(ss, e, _, _) =>
      ss.forall {
        case _: DefDef => false
        case v: ValDef => !v.symbol.flags.is(Flags.Mutable) && !v.symbol.flags.is(Flags.Lazy)
        case _ => true
      } && headable(e)
    case IR.Bind(io, _, b, strict, _) => headable(io) && (!strict || headable(b))
    case IR.BindF(io, _, _)           => headable(io)
    case IR.Map(io, _, _, _, _)       => headable(io)
    case IR.MapF(io, _, _)            => headable(io)
    case IR.Cond(_, a, b, _, _)       => headable(a) && headable(b)
    case IR.Switch(_, cs, _, _)       => cs.forall(c => headable(c._3))
    case IR.Handle(io, _, _, _, _)    => headable(io)
    case IR.Attempt(io, _, _)         => headable(io)
    case _                            => true

  private def pack(vals: List[Symbol]): Term = vals match
    case Nil => Literal(NullConstant())
    case List(one) if !primitive(one.termRef.widen) => castTo(Ref(one), TypeRepr.of[AnyRef])
    case _ => '{ Array[AnyRef](${ Varargs(vals.map(v => castTo(Ref(v), TypeRepr.of[AnyRef]).asExprOf[AnyRef])) }*) }.asTerm

  private def unpack(plan: Term, vals: List[Symbol], j: Int): Term = vals match
    case List(one) if !primitive(one.termRef.widen) => castTo(plan, one.termRef.widen)
    case _ =>
      val arr = castTo(plan, TypeRepr.of[Array[AnyRef]]).asExprOf[Array[AnyRef]]
      castTo('{ $arr(${ Expr(j) }) }.asTerm, vals(j).termRef.widen)

  /** A def that could not be split, used as a value: its body is built whenever the effect runs. */
  private def noteValue(i: Ident): Unit = noteLate(i.symbol, i.pos)

  private def noteLate(f: Symbol, pos: Position): Unit =
    if startsBuilding(f) && !isSplit(f) && noted.add((pos.sourceFile.path, pos.start)) then
      val msg = s"strom-effect: the effect `${f.name}(..)` is used as a value here. The plain code builds the start of `${f.name}`'s body " +
        s"once, at this call; the fused code builds it each time the effect runs (the body starts with a local def, a var " +
        s"or a lazy val, which keeps strom-effect from building it ahead)."
      notes += msg
      if !quiet && !Impl.notesOff then report.info(msg, pos)

  private val noted = scala.collection.mutable.Set.empty[(String, Int)]

  /** Where the fused code builds later than the plain code: a local def that could not be split
   *  (see [[isSplit]]), called by value after an effect of the same region (`a *> loop(i)`). The
   *  plain code calls it, and builds the start of its body, before `a` runs. We say so. Returns
   *  whether an effect may have run. */
  private def noteCalls(ir: IR, ran: Boolean): Boolean = ir match
    case IR.Call(f, _, _, _) =>
      val pos = callPos.get(ir)
      if ran && (pos ne null) && startsBuilding(f) && !isSplit(f) && noted.add((pos.sourceFile.path, pos.start)) then
        val msg = s"strom-effect: `${f.name}` is called by value after an effect of the same expression (as in `a *> ${f.name}(..)`). " +
          s"The plain code builds the start of `${f.name}`'s body before that effect runs; the fused code builds it after. " +
          s"Write `a >> ${f.name}(..)` or `a.flatMap(_ => ${f.name}(..))` to get the same order in both."
        notes += msg
        if !quiet && !Impl.notesOff then report.info(msg, pos)
      true
    case _: IR.Pure                      => ran
    case _: IR.Delay | _: IR.Opaque | _: IR.Raise | _: IR.Lazy => true
    case IR.Bind(io, _, b, strict, _)    => val r = noteCalls(io, ran); if strict then noteCalls(b, r) else true
    case IR.BindF(io, _, _)              => noteCalls(io, ran); true
    case IR.Map(io, _, _, _, _)          => noteCalls(io, ran)
    case IR.MapF(io, _, _)               => noteCalls(io, ran)
    case IR.Stats(_, e, _, _)            => noteCalls(e, ran)
    case IR.Cond(_, a, b, _, _)          => val x = noteCalls(a, ran); val y = noteCalls(b, ran); x || y
    case IR.Switch(_, cs, _, _)          => cs.map(c => noteCalls(c._3, ran)).contains(true)
    case IR.Handle(io, _, _, _, _)       => noteCalls(io, ran); true
    case IR.Attempt(io, _, _)            => noteCalls(io, ran); true
    case IR.Chosen(_, cs, _)             => cs.map(noteCalls(_, ran)).contains(true)

  /** Emits a region: as it is when the order is already the plain code's, hoisted otherwise. */
  private def region(ir: IR)(emit: (IR, Ctx) => Term)(using c: Ctx): Term =
    noteCalls(ir, ran = false)
    if !order(ir, ran = false).need then emit(ir, c)
    else
      val h = Hoist(c)
      val ir2 = h.go(ir)
      val out = h.statements
      if out.isEmpty then emit(ir2, h.ctx) else Block(out, emit(ir2, h.ctx))

  private def directR(ir: IR)(using Ctx): Term = region(ir)((i, c2) => direct(i)(using c2))
  private def cpsR(ir: IR, k: K)(using Ctx): Term = region(ir)((i, c2) => cps(i, k)(using c2))

  /** The effect for the whole block. Unlike an inner region it is built once and may run many
   *  times, so everything it builds is built here, outside the deferred code that runs it. */
  private def build(ir: IR)(using c: Ctx): Term = ir match
    case IR.Stats(ss, e, ds, _)   => stats(ss, ds)(build(e))
    case IR.Cond(cnd, a, b, _, _) => If(user(cnd), build(a), build(b))
    case IR.Switch(s, cs, _, _)   => Match(user(s), cs.map((p, g, b) => caseDef(p, g)((c2: Ctx) ?=> build(b)(using c2))))
    case _ =>
      noteCalls(ir, ran = false)
      val h = Hoist(c, once = true)
      val ir2 = h.go(ir)
      val body = ir2 match
        case IR.Opaque(t, _) => opq(t)(using h.ctx) // already an effect value
        case _               => later(cps(ir2, K.Ret)(using h.ctx), c.r)(using h.ctx)
      val out = h.statements
      if out.isEmpty then body else Block(out, body)

  private def methodType(d: DefDef, plan: Boolean = false, result: Option[TypeRepr] = None): MethodType =
    val ps = d.termParamss.head.params
    val extra = if plan then List("plan" -> TypeRepr.of[AnyRef]) else Nil
    MethodType(ps.map(_.name) ++ extra.map(_._1))(_ => ps.map(_.tpt.tpe) ++ extra.map(_._2), _ => result.getOrElse(d.returnTpt.tpe))

  private def zero(tp: TypeRepr): Term = withT(tp)([t] => (_: Type[t]) ?=> '{ null.asInstanceOf[t] }.asTerm)

  private def castTo(t: Term, tp: TypeRepr): Term = TypeApply(Select.unique(t, "asInstanceOf"), List(Inferred(tp)))
  private def int(i: Int): Term = Literal(IntConstant(i))

  /** `def f(p..): F[X] = body` becomes `f$run`:
   *  `{ var p'..; var n = 0; var res = null; while res eq null do { val p = p'..; res = body' }; res }`
   *  where a synchronous self-call in tail position assigns the vars and yields null. A split def (see
   *  [[isSplit]]) also gets `f$head`, and its `f$run` takes the plan the head built. */
  private def compileDef(d: DefDef, sym: Symbol, defs: Set[Symbol], head: Option[Symbol])(using c: Ctx): List[DefDef] =
    val (fx, x) = split(d.returnTpt.tpe).get
    val params = d.termParamss.head.params
    val ir = bodyOf(d, defs)
    // The head: everything the start of the body builds, as vals, and those vals as the plan.
    // built: the vals, the body rewritten to use them, and the user's vals among them.
    var built: (List[Symbol], IR, Map[Symbol, Symbol]) = null
    val headDef = head.map { h =>
      DefDef(h, {
        case List(ps) =>
          val hc = Ctx(h, fx, x, None, async = false, inDef = true,
                       sub = c.sub ++ params.map(_.symbol).zip(ps.map(_.symbol)), runs = c.runs, heads = c.heads)
          val hoist = Hoist(hc)
          val ir2 = hoist.go(ir)
          val out = hoist.statements
          val vals = out.collect { case v: ValDef => v.symbol }
          built = (vals, ir2, hoist.ctx.sub.filter((u, v) => !hc.sub.contains(u) && vals.contains(v)))
          Some(Block(out, pack(vals)))
        case _ => None
      })
    }
    val body = if built eq null then ir else built._2
    val names = params.map(_.name) ++ head.map(_ => "plan")
    val types = params.map(_.tpt.tpe) ++ head.map(_ => TypeRepr.of[AnyRef])
    val runDef = DefDef(sym, {
      case List(ps) =>
        val vars = names.zip(types).map((nm, tp) => Symbol.newVal(sym, name(nm), tp, Flags.Mutable, Symbol.noSymbol))
        val n    = Symbol.newVal(sym, name("n"), TypeRepr.of[Int], Flags.Mutable, Symbol.noSymbol)
        /** One iteration: the parameters as vals (copies of the vars), what the head built, the body. */
        def iteration(owner: Symbol): Term =
          val its = names.zip(types).map((nm, tp) => Symbol.newVal(owner, name(nm), tp, Flags.EmptyFlags, Symbol.noSymbol))
          val copies = its.zip(vars).map((i, v) => ValDef(i, Some(Ref(v))))
          val base = Ctx(owner, fx, x, Some(Loop(d.symbol, sym, vars, n)), async = false, inDef = true,
                         sub = c.sub ++ params.map(_.symbol).zip(its), runs = c.runs, heads = c.heads)
          if built eq null then Block(copies, cpsR(ir, K.Ret)(using base))
          else
            val (vals, ir2, users) = built
            val loads = vals.zipWithIndex.map { (v, j) =>
              (v, Symbol.newVal(owner, name(v.name.takeWhile(_ != '$')), v.termRef.widen, Flags.EmptyFlags, Symbol.noSymbol), j) }
            val local = loads.map((v, l, _) => v -> l).toMap
            val ctx = base.copy(sub = base.sub ++ local ++ users.map((u, v) => u -> local(v)))
            val load = loads.map((_, l, j) => ValDef(l, Some(unpack(Ref(its.last), vals, j))))
            Block(copies ++ load, cps(ir2, K.Ret)(using ctx))
        given Ctx = c.copy(owner = sym, fx = fx)
        val init = vars.zip(ps).map((v, p) => ValDef(v, Some(Ref(p.symbol)))) :+ ValDef(n, Some(int(0)))
        val res = Symbol.newVal(sym, name("res"), B.effect(fx, x), Flags.Mutable, Symbol.noSymbol)
        val resE = Ref(res).asExprOf[AnyRef]
        val loop = Block(List(ValDef(res, Some(nullIO)), While('{ $resE eq null }.asTerm, Assign(Ref(res), iteration(sym)))), Ref(res))
        Some(B.boundary(Block(init, loop), x))
      case _ => None
    })
    headDef.toList :+ runDef

  def run(e: Expr[Any]): Expr[Any] =
    if Impl.fuseOff then return e
    val t = e.asTerm
    val ir = parse(t)(using Set.empty)
    val (fx, a) = split(t.tpe).get
    given Ctx = Ctx(Symbol.spliceOwner, fx, a, None, async = false, inDef = false, Map.empty)
    ir match
      case IR.Opaque(o, _) =>
        val o2 = fuseArgs(o)
        if o2 eq o then e else user(o2).asExpr // nothing to fuse, or only inside the call's arguments
      case _ if gate && savings(ir)(using Set.empty) == 0 => e // fusing would save nothing: as written
      case _ => build(ir).asExpr
