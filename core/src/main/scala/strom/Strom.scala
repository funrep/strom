package strom

import strom.runtime.{DirectFrame, ParallelPlan, SegmentFactory}

/** A fusible stream description.
 *
 *  `Strom` values are ordinary immutable trees, so the same expression can be run by the naive
 *  [[Reference]] interpreter. The staged entry points ([[Strom.run]], [[Strom.segment]], …) never
 *  build this tree: they are macros that read the *source* of the expression and compile it into
 *  a fused, allocation-free loop. Pipelines given to them must therefore be written inline
 *  (directly or via `inline def`), with lambda literals for `flatMap`.
 */
sealed abstract class Strom[A]:
  import Strom.Node.*

  def map[B](f: A => B): Strom[B] = MapN(this, f)
  def filter(p: A => Boolean): Strom[A] = FilterN(this, p)
  def flatMap[B](f: A => Strom[B]): Strom[B] = FlatMapN(this, f)
  def take(n: Int): Strom[A] = TakeN(this, n)
  def zipWith[B, C](that: Strom[B])(f: (A, B) => C): Strom[C] = ZipWithN(this, that, f)
  /** Pairs elements. Allocates a tuple per element; prefer [[zipWith]] on the fused path. */
  def zip[B](that: Strom[B]): Strom[(A, B)] = ZipWithN(this, that, (a, b) => (a, b))
  /** Maps the elements `pf` is defined at, drops the others. A literal is staged as a `match`. */
  def collect[B](pf: PartialFunction[A, B]): Strom[B] = CollectN(this, pf)
  def mapFilter[B](f: A => Option[B]): Strom[B] = MapFilterN(this, f)
  /** Pairs elements with their index. Allocates a tuple per element. */
  def zipWithIndex: Strom[(A, Long)] = ZipWithIndexN(this)
  /** Emits `z`, then the running fold (like fs2 / ZIO `scan`). */
  def scan[S](z: S)(op: (S, A) => S): Strom[S] = ScanN(this, z, op)
  def drop(n: Long): Strom[A] = DropN(this, n)
  def takeWhile(p: A => Boolean): Strom[A] = TakeWhileN(this, p)
  def dropWhile(p: A => Boolean): Strom[A] = DropWhileN(this, p)
  /** Tumbling window of `size` elements, each folded with `op`; a final partial window is emitted. */
  def windowFold[S](size: Int)(z: S)(op: (S, A) => S): Strom[S] = WindowFoldN(this, size, z, op)

  def fold[S](z: S)(op: (S, A) => S): Fold[S] = Fold(this, z, op, associative = false)
  /** Like [[fold]], but declares `op` associative (with `z` as identity), enabling parallel folding. */
  def foldAssociative[S](z: S)(op: (S, A) => S): Fold[S] = Fold(this, z, op, associative = true)
  def sum(using num: Numeric[A]): Fold[A] = Fold(this, num.zero, num.plus, associative = true)
  def count: Fold[Long] = Fold(this, 0L, (n, _) => n + 1, associative = true)

/** A terminal fold over a [[Strom]]. */
final class Fold[S] private (val src: Strom[?], val z: S, val op: (S, Any) => S, val associative: Boolean)
object Fold:
  private[strom] def apply[S, A](src: Strom[A], z: S, op: (S, A) => S, associative: Boolean): Fold[S] =
    new Fold(src, z, op.asInstanceOf[(S, Any) => S], associative)

object Strom:
  def fromArray[A](array: Array[A]): Strom[A] = Node.ArrayN(array)
  /** Integers `from until until`. */
  def range(from: Int, until: Int): Strom[Int] = Node.RangeN(from, until)
  /** Reference-interpreter-only source (not stageable). */
  def fromSeq[A](xs: Seq[A]): Strom[A] = Node.SeqN(xs)

  private[strom] object Node:
    final case class ArrayN[A](array: Array[A]) extends Strom[A]
    final case class RangeN(from: Int, until: Int) extends Strom[Int]
    final case class SeqN[A](xs: Seq[A]) extends Strom[A]
    final case class MapN[A, B](up: Strom[A], f: A => B) extends Strom[B]
    final case class FilterN[A](up: Strom[A], p: A => Boolean) extends Strom[A]
    final case class FlatMapN[A, B](up: Strom[A], f: A => Strom[B]) extends Strom[B]
    final case class TakeN[A](up: Strom[A], n: Int) extends Strom[A]
    final case class ZipWithN[A, B, C](l: Strom[A], r: Strom[B], f: (A, B) => C) extends Strom[C]
    final case class CollectN[A, B](up: Strom[A], pf: PartialFunction[A, B]) extends Strom[B]
    final case class MapFilterN[A, B](up: Strom[A], f: A => Option[B]) extends Strom[B]
    final case class ZipWithIndexN[A](up: Strom[A]) extends Strom[(A, Long)]
    final case class ScanN[A, S](up: Strom[A], z: S, op: (S, A) => S) extends Strom[S]
    final case class DropN[A](up: Strom[A], n: Long) extends Strom[A]
    final case class TakeWhileN[A](up: Strom[A], p: A => Boolean) extends Strom[A]
    final case class DropWhileN[A](up: Strom[A], p: A => Boolean) extends Strom[A]
    final case class WindowFoldN[A, S](up: Strom[A], size: Int, z: S, op: (S, A) => S) extends Strom[S]

  // ------------------------------------------------------------------ staged entry points

  /** Compiles a pipeline over arrays/ranges into a single fused loop and runs it. */
  inline def run[R](inline fold: Fold[R]): R = ${ internal.Macros.run[R]('fold) }

  /** Compiles a one-input pipeline into a resumable segment emitting `B`s. */
  inline def segment[A, B](inline f: Strom[A] => Strom[B]): SegmentFactory[B] =
    ${ internal.Macros.segment[B]('f) }
  /** Compiles a two-input pipeline into a resumable segment emitting `C`s. */
  inline def segment2[A, B, C](inline f: (Strom[A], Strom[B]) => Strom[C]): SegmentFactory[C] =
    ${ internal.Macros.segment[C]('f) }
  /** Compiles a one-input fold into a resumable segment whose `result` is an `R`. */
  inline def foldSegment[A, R](inline f: Strom[A] => Fold[R]): SegmentFactory[R] =
    ${ internal.Macros.segment[R]('f) }
  /** Compiles a two-input fold into a resumable segment whose `result` is an `R`. */
  inline def foldSegment2[A, B, R](inline f: (Strom[A], Strom[B]) => Fold[R]): SegmentFactory[R] =
    ${ internal.Macros.segment[R]('f) }

  /** A parallel execution plan for a one-input pipeline (see [[strom.runtime.ParallelPlan]]):
   *  its stateless prefix is replicated, the rest runs as a single instance. */
  inline def parallel[A, B](inline f: Strom[A] => Strom[B]): ParallelPlan[B] =
    ${ internal.Macros.parallel[B]('f) }
  /** As [[parallel]] for a fold; an associative fold runs as per-chunk partial folds. */
  inline def parallelFold[A, R](inline f: Strom[A] => Fold[R]): ParallelPlan[R] =
    ${ internal.Macros.parallel[R]('f) }
  /** A stateful pipeline run as one instance per partition; `key` routes elements so that all
   *  elements with the same key reach the same instance, in order. */
  inline def parallelKeyed[A, B](inline key: A => Int)(inline f: Strom[A] => Strom[B]): ParallelPlan[B] =
    ${ internal.Macros.parallelKeyed[B]('f, 'key) }

  /** The generated code (`Expr.show`) for a pipeline, as a compile-time constant.
   *  Accepts a [[Fold]] (direct mode) or a lambda with explicitly typed `Strom` parameters
   *  (segment mode). */
  inline def showCode(inline pipeline: Any): String = ${ internal.Macros.showCode('pipeline) }

  /** Static metadata and the Kiselyov IR shape of a pipeline, as a compile-time constant. */
  inline def explain(inline pipeline: Any): String = ${ internal.Macros.explain('pipeline) }
