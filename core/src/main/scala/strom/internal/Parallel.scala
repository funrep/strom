package strom.internal

import scala.quoted.*
import strom.runtime.{ParallelPlan, SegmentFactory, Tier}

/** Plans the parallel execution of a one-input pipeline from its static metadata. Along the
 *  operators from the input port: stateless runs are replicated over N workers; each stateful
 *  operator starts a single-instance tier (cut), which keeps light stateless operators after it
 *  fused while heavy ones (an expansion, or several operators) get their own replicated tier;
 *  an associative fold on a replicated run becomes per-chunk partial folds. */
object Parallel:
  import Pipe.*

  private def stateless(p: Pipe[?]): Boolean = stageInfo(p).stateless

  /** `p` with the subtree `target` (by identity) replaced by `repl`. */
  def replace(p: Pipe[?], target: Pipe[?], repl: Pipe[?])(using Quotes): Pipe[?] =
    if p eq target then repl
    else p match
      case u: Unary[a, b] => u.withUp(replace(u.up, target, repl).asInstanceOf[Pipe[a]])
      case m: ZipWithP[a, b, c] =>
        given Type[c] = m.tpe
        ZipWithP[a, b, c](replace(m.l, target, repl).asInstanceOf[Pipe[a]], replace(m.r, target, repl).asInstanceOf[Pipe[b]], m.f)
      case _ => p

  def outType(sink: Sink): Type[?] = sink match
    case fs: FoldSink[?, ?] => fs.tpe
    case es: EmitSink[?]    => es.tpe

  private def factory(sink: Sink, port: Type[?])(using Quotes): Expr[SegmentFactory[?]] =
    outType(sink) match
      case '[o] => Codegen.segment[o](sink, List(port))

  def plan[O: Type](sink: Sink, portTypes: List[Type[?]])(using Quotes): Expr[ParallelPlan[O]] =
    if portTypes.size != 1 then
      quotes.reflect.report.errorAndAbort("strom: parallel plans support one-input pipelines")
    val tiers = cutPlan(sink, portTypes.head).map(_._2)
    '{ ParallelPlan[O](Vector(${ Varargs(tiers) }*), 1) }

  /** The tiers of the plan for a one-input `sink`, e.g. `replicated[map → filter] | single[take]`;
   *  None when nothing would run in parallel (every tier single). */
  def describe(sink: Sink, port: Type[?])(using Quotes): Option[String] =
    val tiers = cutPlan(sink, port).map(_._1)
    if tiers.forall(_.startsWith("single")) then None else Some(tiers.mkString(" | "))

  // ------------------------------------------------------------------ multi-cut planning

  private def containsPort(p: Pipe[?]): Boolean = p match
    case _: SrcPort[?] => true
    case _: SrcArray[?] | _: SrcRange | _: SrcOne[?] | _: SrcIndexed[?, ?] | _: SrcIter[?] => false
    case ZipWithP(l, r, _) => containsPort(l) || containsPort(r)
    case u: Unary[?, ?] => containsPort(u.up)

  /** The operators from the input port up to `p` (port first). A zip contributes itself; its
   *  other operand stays attached to it. */
  def pathNodes(p: Pipe[?]): List[Pipe[?]] = p match
    case _: SrcPort[?] => List(p)
    case z: ZipWithP[?, ?, ?] => (if containsPort(z.l) then pathNodes(z.l) else pathNodes(z.r)) :+ p
    case u: Unary[?, ?] => pathNodes(u.up) :+ p
    case other => throw IllegalArgumentException(s"no input port in $other")

  private enum Group:
    case Repl(from: Int, to: Int)   // inclusive node indices
    case Single(from: Int, to: Int) // may be empty (to = from - 1): a tier holding only the fold

  /** Stateless operators worth their own replicated tier after a stateful one: an expansion,
   *  or several operators. Lighter runs stay fused with the preceding stateful operator. */
  private def heavy(run: Seq[Pipe[?]]): Boolean = run.exists(_.isInstanceOf[FlatMapP[?, ?]]) || run.size >= 3

  private def cutPlan(sink: Sink, port: Type[?])(using Quotes): List[(String, Expr[Tier])] =
    val nodes = pathNodes(upstream(sink)).toVector
    val n = nodes.size
    val groups = collection.mutable.ArrayBuffer.empty[Group]
    var i = 1
    while i < n do
      if stateless(nodes(i)) then
        var j = i
        while j + 1 < n && stateless(nodes(j + 1)) do j += 1
        groups += Group.Repl(i, j)
        i = j + 1
      else
        var k = i + 1
        while k < n && stateless(nodes(k)) do k += 1
        val run = nodes.slice(i + 1, k)
        val (from, to, next) = if run.nonEmpty && heavy(run) then (i, i, i + 1) else (i, k - 1, k)
        // Adjacent single-instance tiers gain no parallelism: merge them.
        groups.lastOption match
          case Some(Group.Single(f, _)) => groups(groups.size - 1) = Group.Single(f, to)
          case _                        => groups += Group.Single(from, to)
        i = next

    /** The pipe for nodes `from..to` rebuilt over a fresh input port. */
    def segmentPipe(from: Int, to: Int): Pipe[?] =
      nodes(from - 1).tpe match
        case '[t] => (from to to).foldLeft(SrcPort[t](0): Pipe[?])((cur, m) => replace(nodes(m), nodes(m - 1), cur))
    def portOf(from: Int): Type[?] = if from == 1 then port else nodes(from - 1).tpe
    def emit(p: Pipe[?]): Sink = p match { case q: Pipe[a] => EmitSink[a](q) }

    def names(from: Int, to: Int, fold: Boolean): String =
      ((from to to).map(i => stageInfo(nodes(i)).op) ++ (if fold then List("fold") else Nil)).mkString(" → ")
    val tiers = collection.mutable.ArrayBuffer.empty[(String, Expr[Tier])]
    sink match
      case fs: FoldSink[a, s] =>
        given Type[s] = fs.tpe
        def foldOver(p: Pipe[?]): Sink =
          FoldSink[a, s](p.asInstanceOf[Pipe[a]], fs.z, fs.op, fs.associative, fs.combine)(using fs.tpe)
        val (init, last) = (groups.dropRight(1), groups.lastOption)
        init.foreach(g => tiers += tierOf(g, segmentPipe, portOf, emit, names))
        last match
          case Some(Group.Repl(f, t)) if fs.combine.isDefined =>
            tiers += s"partialFold[${names(f, t, true)}]" ->
              '{ Tier.PartialFold(${ factory(foldOver(segmentPipe(f, t)), portOf(f)) }, ${ combine[s](fs.combine.get) }) }
          case Some(Group.Repl(f, t)) =>
            tiers += tierOf(Group.Repl(f, t), segmentPipe, portOf, emit, names)
            tiers += "single[fold]" -> '{ Tier.Single(${ factory(foldOver(segmentPipe(n, n - 1)), portOf(n)) }) }
          case Some(Group.Single(f, t)) =>
            tiers += s"single[${names(f, t, true)}]" -> '{ Tier.Single(${ factory(foldOver(segmentPipe(f, t)), portOf(f)) }) }
          case None =>
            if fs.combine.isDefined then
              tiers += "partialFold[fold]" ->
                '{ Tier.PartialFold(${ factory(foldOver(nodes(0)), port) }, ${ combine[s](fs.combine.get) }) }
            else tiers += "single[fold]" -> '{ Tier.Single(${ factory(foldOver(nodes(0)), port) }) }
      case _: EmitSink[?] =>
        if groups.isEmpty then tiers += "single[]" -> '{ Tier.Single(${ factory(emit(nodes(0)), port) }) }
        else groups.foreach(g => tiers += tierOf(g, segmentPipe, portOf, emit, names))
    tiers.toList

  private def tierOf(g: Group, pipe: (Int, Int) => Pipe[?], portOf: Int => Type[?], emit: Pipe[?] => Sink,
      names: (Int, Int, Boolean) => String)(using Quotes): (String, Expr[Tier]) = g match
    case Group.Repl(f, t)   => s"replicated[${names(f, t, false)}]" -> '{ Tier.Replicated(${ factory(emit(pipe(f, t)), portOf(f)) }) }
    case Group.Single(f, t) => s"single[${names(f, t, false)}]" -> '{ Tier.Single(${ factory(emit(pipe(f, t)), portOf(f)) }) }

  private def combine[S: Type](c: Fn2[S, S, S])(using Quotes): Expr[(Any, Any) => Any] =
    '{ (x: Any, y: Any) => (${ c('{ x.asInstanceOf[S] }, '{ y.asInstanceOf[S] }) }: Any) }
