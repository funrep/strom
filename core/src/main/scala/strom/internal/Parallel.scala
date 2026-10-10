package strom.internal

import scala.quoted.*
import strom.runtime.{KeyPartitioner, ParallelPlan, Partitioner, SegmentFactory, Tier}

/** Plans the parallel execution of a one-input pipeline from its static metadata:
 *  the maximal *stateless* part that reads the input is replicated over N workers; an
 *  associative fold directly on it becomes per-chunk partial folds; everything from the first
 *  stateful operator on runs as a single instance (the pipeline is cut there). */
object Parallel:
  import Pipe.*

  private def stateless(p: Pipe[?]): Boolean = stageInfo(p).stateless

  /** The largest stateless subtree that contains the input port, if `p` contains it. */
  def prefix(p: Pipe[?]): Option[Pipe[?]] = p match
    case _: SrcPort[?] => Some(p)
    case _: SrcArray[?] | _: SrcRange => None
    case MapP(up, _)     => grow(p, up)
    case FilterP(up, _)  => grow(p, up)
    case FlatMapP(up, _) => grow(p, up)
    case TakeP(up, _)    => prefix(up)
    case WindowFoldP(up, _, _, _) => prefix(up)
    case ZipWithP(l, r, _) => prefix(l).orElse(prefix(r))
    case _ => if stateless(p) then grow(p, child(p)) else prefix(child(p))

  private def grow(p: Pipe[?], up: Pipe[?]): Option[Pipe[?]] =
    prefix(up).map(q => if (q eq up) && stateless(p) then p else q)

  /** `p` with the subtree `target` (by identity) replaced by `repl`. */
  def replace(p: Pipe[?], target: Pipe[?], repl: Pipe[?])(using Quotes): Pipe[?] =
    if p eq target then repl
    else p match
      case m: MapP[a, b] =>
        given Type[b] = m.tpe
        MapP[a, b](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.f)
      case m: FilterP[a] =>
        given Type[a] = m.tpe
        FilterP[a](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.p)
      case m: FlatMapP[a, b] =>
        given Type[b] = m.tpe
        FlatMapP[a, b](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.f)
      case m: TakeP[a] =>
        given Type[a] = m.tpe
        TakeP[a](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.n)
      case m: WindowFoldP[a, s] =>
        given Type[s] = m.tpe
        WindowFoldP[a, s](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.size, m.z, m.op)
      case m: CollectP[a, b] =>
        given Type[b] = m.tpe
        CollectP[a, b](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.pf)
      case m: MapOptionP[a, b] =>
        given Type[b] = m.tpe
        MapOptionP[a, b](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.f)
      case m: ZipWithIndexP[a] =>
        given Type[(a, Long)] = m.tpe
        ZipWithIndexP[a](replace(m.up, target, repl).asInstanceOf[Pipe[a]])
      case m: ScanP[a, s] =>
        given Type[s] = m.tpe
        ScanP[a, s](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.z, m.op)
      case m: DropP[a] =>
        given Type[a] = m.tpe
        DropP[a](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.n)
      case m: TakeWhileP[a] =>
        given Type[a] = m.tpe
        TakeWhileP[a](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.p)
      case m: DropWhileP[a] =>
        given Type[a] = m.tpe
        DropWhileP[a](replace(m.up, target, repl).asInstanceOf[Pipe[a]], m.p)
      case m: ZipWithP[a, b, c] =>
        given Type[c] = m.tpe
        ZipWithP[a, b, c](replace(m.l, target, repl).asInstanceOf[Pipe[a]], replace(m.r, target, repl).asInstanceOf[Pipe[b]], m.f)
      case _ => p

  private def replaceIn(sink: Sink, target: Pipe[?], repl: Pipe[?])(using Quotes): Sink = sink match
    case fs: FoldSink[a, s] =>
      FoldSink[a, s](replace(fs.up, target, repl).asInstanceOf[Pipe[a]], fs.z, fs.op, fs.associative, fs.combine)(using fs.tpe)
    case es: EmitSink[a] => EmitSink[a](replace(es.up, target, repl).asInstanceOf[Pipe[a]])

  private def outType(sink: Sink): Type[?] = sink match
    case fs: FoldSink[?, ?] => fs.tpe
    case es: EmitSink[?]    => es.tpe

  private def factory(sink: Sink, port: Type[?])(using Quotes): Expr[SegmentFactory[?]] =
    outType(sink) match
      case '[o] => Codegen.segment[o](sink, List(port))

  def plan[O: Type](sink: Sink, portTypes: List[Type[?]], key: Option[Expr[Any]], front: Frontend)(
      using Quotes): Expr[ParallelPlan[O]] =
    if portTypes.size != 1 then
      quotes.reflect.report.errorAndAbort("strom: parallel plans support one-input pipelines")
    val port = portTypes.head
    val tiers: List[Expr[Tier]] = key match
      case Some(k) =>
        port match
          case '[a] =>
            val keyFn = front.fn1[a, Int](k.asExprOf[a => Int])
            List('{ Tier.Keyed(${ factory(sink, port) }, ${ partitioner[a](keyFn) }) })
      case None =>
        val up = upstream(sink)
        val pre = prefix(up).get
        sink match
          case fs: FoldSink[a, s] if (fs.up eq pre) && fs.combine.isDefined =>
            given Type[s] = fs.tpe
            List('{ Tier.PartialFold(${ factory(sink, port) }, ${ combine[s](fs.combine.get) }) })
          case _: EmitSink[?] if up eq pre =>
            List('{ Tier.Replicated(${ factory(sink, port) }) })
          case _ if pre.isInstanceOf[SrcPort[?]] =>
            List('{ Tier.Single(${ factory(sink, port) }) })
          case _ =>
            pre.tpe match
              case '[p] =>
                val rest = replaceIn(sink, pre, SrcPort[p](0))
                List('{ Tier.Replicated(${ factory(EmitSink(pre), port) }) },
                     '{ Tier.Single(${ factory(rest, pre.tpe) }) })
    '{ ParallelPlan[O](Vector(${ Varargs(tiers) }*), 1) }

  private def combine[S: Type](c: Fn2[S, S, S])(using Quotes): Expr[(Any, Any) => Any] =
    '{ (x: Any, y: Any) => (${ c('{ x.asInstanceOf[S] }, '{ y.asInstanceOf[S] }) }: Any) }

  /** A partitioner specialised for the element type, with the key function inlined. */
  def partitioner[A: Type](key: Fn1[A, Int])(using Quotes): Expr[Partitioner] =
    '{
      new Partitioner {
        def route(array: AnyRef, offset: Int, length: Int, parts: Int): Array[AnyRef] = {
          val in = array.asInstanceOf[Array[A]]
          val part = new Array[Int](length)
          val counts = new Array[Int](parts)
          var i = 0
          while (i < length) {
            val k = KeyPartitioner.partition(${ key('{ in(offset + i) }) }, parts)
            part(i) = k
            counts(k) += 1
            i += 1
          }
          val outs = new Array[AnyRef](parts)
          var j = 0
          while (j < parts) {
            outs(j) = ${ Codegen.newArray[A]('{ counts(j) }) }
            counts(j) = 0
            j += 1
          }
          i = 0
          while (i < length) {
            val k = part(i)
            outs(k).asInstanceOf[Array[A]](counts(k)) = in(offset + i)
            counts(k) += 1
            i += 1
          }
          outs
        }
      }
    }
