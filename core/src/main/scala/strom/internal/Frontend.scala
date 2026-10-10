package strom.internal

import scala.quoted.*
import scala.collection.mutable
import strom.{Fold, Strom}

/** Reads the source tree of a `Strom` expression and builds the [[Pipe]] IR.
 *
 *  User lambdas are beta-reduced by substituting their parameter with the element code, so the
 *  generated loops contain the lambda bodies directly (no closure, no `Function1.apply`, no
 *  boxing). `ports` maps the parameters of a segment lambda to input port numbers. */
final class Frontend(using val q: Quotes):
  import q.reflect.*

  private var ports = Map.empty[Symbol, Int]

  private val portUses = mutable.Map.empty[Int, Int].withDefaultValue(0)

  def strip(t: Term): Term = t match
    case Inlined(_, Nil, e) => strip(e)
    // `inline def p(s: Strom[Int]) = …` binds its argument to a proxy val; a pipeline value (or a
    // stable term) can simply be substituted back.
    case Inlined(_, bindings, e) if bindings.forall(substitutable) =>
      val vals = bindings.collect { case v: ValDef => v }
      strip(Frontend.subst(e, vals.map(_.symbol), vals.map(_.rhs.get.asExpr)).asInstanceOf[Term])
    case Typed(e, _)        => strip(e)
    case Block(Nil, e)      => strip(e)
    case _                  => t

  private def substitutable(d: Definition): Boolean = d match
    case ValDef(_, tpt, Some(rhs)) if !d.symbol.flags.is(Flags.Mutable) =>
      tpt.tpe <:< TypeRepr.of[Strom[?]] || tpt.tpe <:< TypeRepr.of[Fold[?]] || (strip(rhs) match
        case Literal(_) | Ident(_) => true
        case _ => false)
    case _ => false

  private def fail(msg: String, t: Tree): Nothing = report.errorAndAbort(msg, t.pos)

  private def warnNotLiteral(f: Term): Unit =
    report.warning("strom: function is not a lambda literal; it is called through Function.apply instead of being inlined", f.pos)

  def fn1[A: Type, B: Type](f: Expr[A => B]): Pipe.Fn1[A, B] = strip(f.asTerm) match
    case Lambda(List(p), body) => Pipe.Fn1(x => Frontend.subst(body, List(p.symbol), List(x)).asExprOf[B])
    case other =>
      warnNotLiteral(other)
      Pipe.Fn1(x => Expr.betaReduce('{ $f($x) }))

  def fn2[A: Type, B: Type, C: Type](f: Expr[(A, B) => C]): Pipe.Fn2[A, B, C] = strip(f.asTerm) match
    case Lambda(List(p1, p2), body) =>
      Pipe.Fn2((x, y) => Frontend.subst(body, List(p1.symbol, p2.symbol), List(x, y)).asExprOf[C])
    case other =>
      warnNotLiteral(other)
      Pipe.Fn2((x, y) => Expr.betaReduce('{ $f($x, $y) }))

  def parse(t0: Term, inner: Boolean): Pipe[?] =
    val t = strip(t0)
    t match
      case Ident(_) if ports.contains(t.symbol) =>
        val n = ports(t.symbol)
        if inner then fail("an input port cannot be used inside flatMap (fan-out is not fused)", t)
        portUses(n) += 1
        if portUses(n) > 1 then fail("an input port can be consumed only once (fan-out is not fused)", t)
        t.tpe.widen.asType match
          case '[Strom[a]] => Pipe.SrcPort[a](n)
          case _           => fail("unexpected port type", t)
      case _ =>
        t.asExpr match
          case '{ Strom.fromArray[a]($arr) } => Pipe.SrcArray[a](arr)
          case '{ Strom.range($from, $until) } => Pipe.SrcRange(from, until)
          case '{ Strom.fromSeq[a]($xs) } =>
            fail("Strom.fromSeq is only supported by the reference interpreter; use fromArray", t)
          case '{ ($s: Strom[a]).map[b]($f) } => Pipe.MapP[a, b](parse(s.asTerm, inner).asInstanceOf[Pipe[a]], fn1(f))
          case '{ ($s: Strom[a]).filter($p) } => Pipe.FilterP[a](parse(s.asTerm, inner).asInstanceOf[Pipe[a]], fn1(p))
          case '{ ($s: Strom[a]).take($n) } => Pipe.TakeP[a](parse(s.asTerm, inner).asInstanceOf[Pipe[a]], '{ $n.toLong })
          case '{ ($s: Strom[a]).flatMap[b]($f) } =>
            val up = parse(s.asTerm, inner).asInstanceOf[Pipe[a]]
            strip(f.asTerm) match
              case Lambda(List(p), body) =>
                Pipe.FlatMapP[a, b](up, Pipe.Inner { x =>
                  val t = Frontend.subst(body, List(p.symbol), List(x))
                  parse(t.asInstanceOf[Term], inner = true).asInstanceOf[Pipe[b]]
                })
              case other => fail("flatMap needs a lambda literal whose body is a Strom expression", other)
          case '{ ($l: Strom[a]).zipWith[b, c]($r)($f) } =>
            Pipe.ZipWithP[a, b, c](parse(l.asTerm, inner).asInstanceOf[Pipe[a]], parse(r.asTerm, inner).asInstanceOf[Pipe[b]], fn2(f))
          case '{ ($l: Strom[a]).zip[b]($r) } =>
            Pipe.ZipWithP[a, b, (a, b)](parse(l.asTerm, inner).asInstanceOf[Pipe[a]], parse(r.asTerm, inner).asInstanceOf[Pipe[b]],
              Pipe.Fn2((x, y) => '{ ($x, $y) }))
          case '{ ($s: Strom[a]).windowFold[st]($size)($z)($op) } =>
            Pipe.WindowFoldP[a, st](parse(s.asTerm, inner).asInstanceOf[Pipe[a]], size, z, fn2(op))
          case _ =>
            fail(s"strom: cannot stage this expression; expected a Strom pipeline written inline, got:\n${t.show}", t)

  /** A segment lambda `(a: Strom[A], b: Strom[B]) => …`: its parameters become input ports.
   *  Returns the port element types and the sink, or None if `f` is not a lambda. */
  def segmentLambda(f: Expr[Any]): Option[(List[Type[?]], Pipe.Sink)] =
    strip(f.asTerm) match
      case Lambda(params, body) =>
        if params.size > strom.runtime.Segment.MaxPorts then
          report.errorAndAbort(s"strom: at most ${strom.runtime.Segment.MaxPorts} inputs are supported")
        val portTypes = params.map { p =>
          p.tpt.tpe.widen.asType match
            case '[Strom[a]] => Type.of[a]
            case _ => fail("strom: segment parameters must have type Strom[A]", p)
        }
        ports = params.map(_.symbol).zipWithIndex.toMap
        Some((portTypes, parseSink(body)))
      case _ => None

  def parseSink(e: Expr[Any]): Pipe.Sink = parseSink(e.asTerm)

  def parseSink(t0: Term): Pipe.Sink =
    val t = strip(t0)
    t.asExpr match
      case '{ ($s: Strom[a]).fold[r]($z)($op) } =>
        Pipe.FoldSink[a, r](parse(s.asTerm, false).asInstanceOf[Pipe[a]], z, fn2(op), associative = false)
      case '{ ($s: Strom[a]).foldAssociative[r]($z)($op) } =>
        val combine =
          if TypeRepr.of[a] =:= TypeRepr.of[r] then Some(fn2(op).asInstanceOf[Pipe.Fn2[r, r, r]]) else None
        Pipe.FoldSink[a, r](parse(s.asTerm, false).asInstanceOf[Pipe[a]], z, fn2(op), associative = true, combine)
      case '{ ($s: Strom[a]).sum(using $num) } =>
        val up = parse(s.asTerm, false).asInstanceOf[Pipe[a]]
        Pipe.FoldSink[a, a](up, sumZero[a](num), sumOp[a](num), associative = true, Some(sumOp[a](num)))
      case '{ ($s: Strom[a]).count } =>
        Pipe.FoldSink[a, Long](parse(s.asTerm, false).asInstanceOf[Pipe[a]], '{ 0L }, Pipe.Fn2((acc, _) => '{ $acc + 1L }),
          associative = true, Some(Pipe.Fn2((x, y) => '{ $x + $y })))
      case '{ $s: Strom[a] } => Pipe.EmitSink[a](parse(s.asTerm, false).asInstanceOf[Pipe[a]])
      case '{ $f: Fold[r] } => fail("strom: unsupported fold; expected fold/foldAssociative/sum/count written inline", t)
      case _ => fail("strom: expected a Strom or Fold expression", t)

  /** `sum` specialised from the element type: primitive `+` for primitives, `Numeric` otherwise. */
  private def sumZero[A: Type](num: Expr[Numeric[A]]): Expr[A] = Type.of[A] match
    case '[Int]    => '{ 0 }.asExprOf[A]
    case '[Long]   => '{ 0L }.asExprOf[A]
    case '[Double] => '{ 0.0 }.asExprOf[A]
    case '[Float]  => '{ 0f }.asExprOf[A]
    case _         => '{ $num.zero }

  private def sumOp[A: Type](num: Expr[Numeric[A]]): Pipe.Fn2[A, A, A] = Type.of[A] match
    case '[Int]    => Pipe.Fn2((a, b) => '{ ${ a.asExprOf[Int] } + ${ b.asExprOf[Int] } }.asExprOf[A])
    case '[Long]   => Pipe.Fn2((a, b) => '{ ${ a.asExprOf[Long] } + ${ b.asExprOf[Long] } }.asExprOf[A])
    case '[Double] => Pipe.Fn2((a, b) => '{ ${ a.asExprOf[Double] } + ${ b.asExprOf[Double] } }.asExprOf[A])
    case '[Float]  => Pipe.Fn2((a, b) => '{ ${ a.asExprOf[Float] } + ${ b.asExprOf[Float] } }.asExprOf[A])
    case _         => Pipe.Fn2((a, b) => '{ $num.plus($a, $b) })

object Frontend:
  /** Beta-reduction by substitution: replaces references to `params` in `body` with `args`.
   *  Runs with the `Quotes` of the splice the result is used in; `body` and `params` come
   *  from the (outer) macro invocation, which is visible from any inner splice. */
  def subst(using q: Quotes)(body: Any, params: List[Any], args: List[Expr[?]]): q.reflect.Term =
    import q.reflect.*
    val m: Map[Symbol, Term] = params.asInstanceOf[List[Symbol]].zip(args.map(_.asTerm)).toMap
    val mapper = new TreeMap:
      override def transformTerm(t: Term)(owner: Symbol): Term = t match
        case Ident(_) if m.contains(t.symbol) => m(t.symbol)
        case _ => super.transformTerm(t)(owner)
    mapper.transformTerm(body.asInstanceOf[Term])(Symbol.spliceOwner)
