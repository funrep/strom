package strom.internal

import scala.quoted.*

/** Staging-time tuples: a tuple built from plain variables is never allocated if all its uses
 *  are projections or tuple patterns. `simplify` removes those uses after beta reduction:
 *    - `TupleN(a, b)._1`                         → `a`
 *    - `TupleN(a, b) match { case (x, y) if g => e … }` → `if (g[x:=a, y:=b]) e[…] else …`
 *  Components must be trivial (variables or literals), so nothing is dropped or duplicated. */
object Tuples:

  /** The components of a tuple constructor `TupleN.apply(a, …)`. */
  def components(using q: Quotes)(t: q.reflect.Term): Option[List[q.reflect.Term]] =
    import q.reflect.*
    t match
      case Inlined(_, Nil, e) => components(e)
      case Typed(e, _)        => components(e)
      case Apply(TypeApply(Select(m, "apply"), _), args) if isTupleModule(m.symbol, args.size) => Some(args)
      case Apply(Select(m, "apply"), args) if isTupleModule(m.symbol, args.size)                => Some(args)
      case _ => None

  private def isTupleModule(using q: Quotes)(s: q.reflect.Symbol, n: Int): Boolean =
    n >= 2 && n <= 22 && s == q.reflect.Symbol.requiredModule(s"scala.Tuple$n")

  def trivial(using q: Quotes)(t: q.reflect.Term): Boolean =
    import q.reflect.*
    t match
      case Literal(_) => true
      case Ident(_)   => t.symbol.isTerm && !t.symbol.flags.is(Flags.Mutable) && !t.symbol.isDefDef
      case Inlined(_, Nil, e) => trivial(e)
      case _ => false

  def simplify(using q: Quotes)(t: q.reflect.Term): q.reflect.Term =
    import q.reflect.*
    val ProjName = "_(\\d+)".r

    /** The sub-patterns of a tuple pattern `(p1, …, pn)`. */
    def tuplePattern(p: Tree): Option[List[Tree]] = p match
      case Unapply(fun, Nil, pats) if isTupleUnapply(fun, pats.size) => Some(pats)
      case TypedOrTest(inner, _) => tuplePattern(inner)
      case _ => None
    def isTupleUnapply(fun: Term, n: Int): Boolean = fun match
      case TypeApply(f, _) => isTupleUnapply(f, n)
      case Select(m, "unapply") => isTupleModule(m.symbol, n)
      case _ => false
    /** A sub-pattern that always matches: `x`, `_`, `x: T`, `_: T`; with its binder if any. */
    def irrefutable(p: Tree): Option[Option[Symbol]] = p match
      case Wildcard() => Some(None)
      case Typed(Wildcard(), _) => Some(None)
      case Bind(_, inner) => irrefutable(inner).map(_ => Some(p.symbol))
      case _ => None

    def substitute(body: Term, m: Map[Symbol, Term]): Term =
      if m.isEmpty then body
      else new TreeMap:
        override def transformTerm(x: Term)(owner: Symbol): Term = x match
          case Ident(_) if m.contains(x.symbol) => m(x.symbol)
          case _ => super.transformTerm(x)(owner)
      .transformTerm(body)(Symbol.spliceOwner)

    /** `sel match { cases }` as an if-chain over the tuple's components, if every case is a
     *  tuple pattern of irrefutable sub-patterns (or `_`) and the last case always matches. */
    def matchToIf(args: List[Term], cases: List[CaseDef]): Option[Term] =
      val decoded = cases.map { c =>
        val binds: Option[Map[Symbol, Term]] = c.pattern match
          case Wildcard() => Some(Map.empty)
          case p => tuplePattern(p).filter(_.size == args.size).flatMap { pats =>
            val subs = pats.map(irrefutable)
            if subs.forall(_.isDefined) then Some(subs.zip(args).collect { case (Some(Some(sym)), a) => sym -> a }.toMap)
            else None
          }
        binds.map(b => (c.guard.map(substitute(_, b)), substitute(c.rhs, b)))
      }
      if decoded.exists(_.isEmpty) then None
      else
        val ds = decoded.flatten
        // Cases after the first unguarded one are unreachable.
        val reachable = ds.indexWhere(_._1.isEmpty) match
          case -1 => None
          case i  => Some(ds.take(i + 1))
        reachable.map { rs =>
          rs.init.foldRight(rs.last._2) { case ((Some(g), body), rest) => If(g, body, rest); case ((None, body), _) => body }
        }

    val mapper = new TreeMap:
      override def transformTerm(x: Term)(owner: Symbol): Term =
        super.transformTerm(x)(owner) match
          case s @ Select(c, ProjName(i)) =>
            components(c) match
              case Some(args) if args.forall(trivial) && i.toInt >= 1 && i.toInt <= args.size => args(i.toInt - 1)
              case _ => s
          case m @ Match(sel, cases) =>
            components(sel) match
              case Some(args) if args.forall(trivial) => matchToIf(args, cases).getOrElse(m)
              case _ => m
          case other => other
    mapper.transformTerm(t)(Symbol.spliceOwner)
