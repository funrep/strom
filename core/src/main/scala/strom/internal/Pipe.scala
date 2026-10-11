package strom.internal

import scala.quoted.*
import strom.runtime.{Cardinality, PlanInfo, StageInfo}

/** The logical pipeline IR, produced by the [[Frontend]] (or built directly by tests).
 *
 *  User functions are *staging-time* functions over code (`Expr[A] => Expr[B]`): applying them
 *  splices the user's lambda body with its parameter substituted (beta reduction), so no
 *  closure survives into the generated code. */
sealed abstract class Pipe[A](using val tpe: Type[A])

object Pipe:
  /** Staging-time functions take the `Quotes` of the splice they are applied in. */
  final class Fn1[A, B](f: Quotes ?=> Expr[A] => Expr[B]):
    def apply(x: Expr[A])(using Quotes): Expr[B] = f(x)
  final class Fn2[A, B, C](f: Quotes ?=> (Expr[A], Expr[B]) => Expr[C]):
    def apply(x: Expr[A], y: Expr[B])(using Quotes): Expr[C] = f(x, y)
  final class Inner[A, B](f: Quotes ?=> Expr[A] => Pipe[B]):
    def apply(x: Expr[A])(using Quotes): Pipe[B] = f(x)
  /** A partial function: calls the continuation with the result at most once. */
  final class Partial[A, B](f: Quotes ?=> (Expr[A], Expr[B] => Expr[Unit]) => Expr[Unit]):
    def apply(x: Expr[A], k: Expr[B] => Expr[Unit])(using Quotes): Expr[Unit] = f(x, k)

  final case class SrcArray[A: Type](arr: Expr[Array[A]]) extends Pipe[A]
  final case class SrcRange(from: Expr[Int], until: Expr[Int])(using Type[Int]) extends Pipe[Int]
  /** One element (`Stream.emit`, `ZStream.succeed`). */
  final case class SrcOne[A: Type](x: Expr[A]) extends Pipe[A]
  /** An indexed collection `coll`, read with `length` and `at` (a chunk, an `IndexedSeq`). */
  final case class SrcIndexed[A: Type, C](coll: Expr[C], length: Fn1[C, Int], at: Fn2[C, Int, A])(
      using val collTpe: Type[C]) extends Pipe[A]
  /** Elements of an iterator (e.g. of a `List`); one iterator per run of the source. */
  final case class SrcIter[A: Type](it: Expr[Iterator[A]]) extends Pipe[A]
  /** An input port of a resumable segment. */
  final case class SrcPort[A: Type](port: Int) extends Pipe[A]
  final case class ZipWithP[A, B, C: Type](l: Pipe[A], r: Pipe[B], f: Fn2[A, B, C]) extends Pipe[C]

  /** An operator with one upstream; `withUp` is the same operator over another upstream. */
  sealed abstract class Unary[A, B: Type] extends Pipe[B]:
    def up: Pipe[A]
    def withUp(u: Pipe[A]): Pipe[B]

  final case class MapP[A, B: Type](up: Pipe[A], f: Fn1[A, B]) extends Unary[A, B]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class FilterP[A: Type](up: Pipe[A], p: Fn1[A, Boolean]) extends Unary[A, A]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class FlatMapP[A, B: Type](up: Pipe[A], f: Inner[A, B]) extends Unary[A, B]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class TakeP[A: Type](up: Pipe[A], n: Expr[Long]) extends Unary[A, A]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class CollectP[A, B: Type](up: Pipe[A], pf: Partial[A, B]) extends Unary[A, B]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class MapOptionP[A, B: Type](up: Pipe[A], f: Fn1[A, Option[B]]) extends Unary[A, B]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class ZipWithIndexP[A](up: Pipe[A])(using Type[(A, Long)]) extends Unary[A, (A, Long)]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class ScanP[A, S: Type](up: Pipe[A], z: Expr[S], op: Fn2[S, A, S]) extends Unary[A, S]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class DropP[A: Type](up: Pipe[A], n: Expr[Long]) extends Unary[A, A]:
    def withUp(u: Pipe[A]) = copy(up = u)
  /** How a `MapAccumP` step's result encodes the output. */
  enum AccumOut:
    /** `(S, Option[B])`: emit when defined. */
    case OptionOut
    /** `(S, B)`: always emit `B`. */
    case AlwaysOut
    /** `(S, B)`: always emit the pair `(S', B)` (fs2 `mapAccumulate`). */
    case PairOut
  final case class MapAccumP[A, S, B: Type](up: Pipe[A], z: Expr[S], f: Fn2[S, A, Any], out: AccumOut)(
      using val stateTpe: Type[S]) extends Unary[A, B]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class TakeWhileP[A: Type](up: Pipe[A], p: Fn1[A, Boolean]) extends Unary[A, A]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class DropWhileP[A: Type](up: Pipe[A], p: Fn1[A, Boolean]) extends Unary[A, A]:
    def withUp(u: Pipe[A]) = copy(up = u)
  final case class WindowFoldP[A, S: Type](up: Pipe[A], size: Expr[Int], z: Expr[S], op: Fn2[S, A, S])
      extends Unary[A, S]:
    def withUp(u: Pipe[A]) = copy(up = u)

  /** What happens to the elements at the end of the pipeline. */
  sealed abstract class Sink
  /** `combine` merges two partial results; it is known for `sum`, `count`, and for
   *  `foldAssociative` whose element type is the result type. It enables parallel partial folds. */
  final case class FoldSink[A, S](up: Pipe[A], z: Expr[S], op: Fn2[S, A, S], associative: Boolean,
                                  combine: Option[Fn2[S, S, S]] = None)(
      using val tpe: Type[S]) extends Sink
  final case class EmitSink[A](up: Pipe[A]) extends Sink:
    def tpe: Type[A] = up.tpe

  def upstream(s: Sink): Pipe[?] = s match
    case f: FoldSink[?, ?] => f.up
    case e: EmitSink[?]    => e.up

  // ------------------------------------------------------------------ static metadata

  def stageInfo(p: Pipe[?]): StageInfo = p match
    case _: SrcArray[?]           => StageInfo("source:array", stateless = true, Cardinality.OneToOne)
    case _: SrcRange              => StageInfo("source:range", stateless = true, Cardinality.OneToOne)
    case _: SrcOne[?]             => StageInfo("source:one", stateless = true, Cardinality.OneToOne)
    case _: SrcIndexed[?, ?]      => StageInfo("source:indexed", stateless = true, Cardinality.OneToOne)
    case _: SrcIter[?]            => StageInfo("source:iterator", stateless = true, Cardinality.OneToOne)
    case SrcPort(n)               => StageInfo(s"source:port$n", stateless = true, Cardinality.OneToOne)
    case _: MapP[?, ?]            => StageInfo("map", stateless = true, Cardinality.OneToOne)
    case _: FilterP[?]            => StageInfo("filter", stateless = true, Cardinality.AtMostOne)
    // The inner stream's state is re-initialised per element, so flatMap carries no state across elements.
    case _: FlatMapP[?, ?]        => StageInfo("flatMap", stateless = true, Cardinality.Many)
    case _: TakeP[?]              => StageInfo("take", stateless = false, Cardinality.Bounded)
    case _: ZipWithP[?, ?, ?]     => StageInfo("zipWith", stateless = false, Cardinality.OneToOne)
    case _: WindowFoldP[?, ?]     => StageInfo("windowFold", stateless = false, Cardinality.AtMostOne)
    case _: CollectP[?, ?]        => StageInfo("collect", stateless = true, Cardinality.AtMostOne)
    case _: MapOptionP[?, ?]      => StageInfo("mapFilter", stateless = true, Cardinality.AtMostOne)
    case _: ZipWithIndexP[?]      => StageInfo("zipWithIndex", stateless = false, Cardinality.OneToOne)
    case _: ScanP[?, ?]           => StageInfo("scan", stateless = false, Cardinality.OneToOne)
    case _: DropP[?]              => StageInfo("drop", stateless = false, Cardinality.AtMostOne)
    case _: MapAccumP[?, ?, ?]    => StageInfo("mapAccum", stateless = false, Cardinality.AtMostOne)
    case _: TakeWhileP[?]         => StageInfo("takeWhile", stateless = false, Cardinality.Bounded)
    case _: DropWhileP[?]         => StageInfo("dropWhile", stateless = false, Cardinality.AtMostOne)

  /** Pre-order listing of the operators of a pipeline. */
  def stages(p: Pipe[?]): List[StageInfo] = (p match
    case u: Unary[?, ?]    => stages(u.up)
    case ZipWithP(l, r, _) => stages(l) ++ stages(r)
    case _                 => Nil) :+ stageInfo(p)

  def planInfo(sink: Sink, inputs: Int): PlanInfo = sink match
    case f: FoldSink[?, ?] =>
      PlanInfo(inputs, stages(f.up) :+ StageInfo("fold", stateless = false, Cardinality.AtMostOne, f.associative), fold = true)
    case e: EmitSink[?] => PlanInfo(inputs, stages(e.up), fold = false)

  given ToExpr[PlanInfo] with
    def apply(p: PlanInfo)(using Quotes): Expr[PlanInfo] =
      val stages = Expr.ofList(p.stages.map { s =>
        '{ StageInfo(${ Expr(s.op) }, ${ Expr(s.stateless) }, Cardinality.fromOrdinal(${ Expr(s.cardinality.ordinal) }),
            ${ Expr(s.associative) }) }
      })
      '{ PlanInfo(${ Expr(p.inputs) }, $stages, ${ Expr(p.fold) }) }
