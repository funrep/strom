package strom.internal

import scala.quoted.*

/** Kiselyov et al.'s staged stream IR ("Stream Fusion, to Completeness", POPL 2017).
 *
 *  Elements are code values (`Expr[A]`). Producers carry an existential state `S` created by a
 *  CPS `init`. All state is allocated through a [[Storage]] (hoisted locals, or slot fields), so a
 *  state value may be used outside the dynamic extent of its `init` — which is what `make_linear`
 *  (zip of nested streams) and resumable segments need. */
enum Card:
  /** Each `step` yields at most one element (possibly none). */
  case AtMost1
  /** Each `step` yields exactly one element. */
  case Many

sealed trait Producer[A]:
  def tpe: Type[A]
  /** Human-readable origin, for `Strom.explain`. */
  var label: String = ""
  /** A composite step (zip / window over pulls) runs inner loops that pay their own fuel, so
   *  the loop around it must not charge fuel: otherwise a tiny budget could be spent before
   *  any real progress is made, and the segment would livelock. */
  var composite: Boolean = false
  /** Whether `step` may suspend the enclosing segment (`return NeedInput`) before yielding. */
  def suspends: Boolean

/** An indexed producer: elements `index(s, i)` for `0 <= i < upb(s)`. Yields exactly one per index. */
final class ForP[A, S](
    val init: (S => Expr[Unit]) => Expr[Unit],
    val upb: S => Expr[Int],
    val index: (S, Expr[Int]) => (Expr[A] => Expr[Unit]) => Expr[Unit],
)(using val tpe: Type[A]) extends Producer[A]:
  def suspends = false

/** A general producer: `step` may be called while `term` holds. */
final class UnfoldP[A, S](
    val init: (S => Expr[Unit]) => Expr[Unit],
    val term: S => Expr[Boolean],
    val card: Card,
    val step: S => (Expr[A] => Expr[Unit]) => Expr[Unit],
    val suspends: Boolean,
)(using val tpe: Type[A]) extends Producer[A]

sealed trait StStream[A]:
  def tpe: Type[A]
/** A single loop. */
final case class Linear[A](p: Producer[A]) extends StStream[A]:
  def tpe = p.tpe
/** An outer producer each of whose elements starts an inner stream (flatMap). */
final case class Nested[A, B](p: Producer[B], f: Expr[B] => StStream[A])(using val tpe: Type[A]) extends StStream[A]

object StStream:
  /** A compact description of the shape of a stream, for `Strom.explain`. */
  def shape(s: StStream[?])(using Quotes): String = s match
    case Linear(p)    => s"Linear(${prod(p)})"
    case n: Nested[?, ?] =>
      n.p.tpe match
        case '[b] =>
          val inner = n.f.asInstanceOf[Expr[b] => StStream[?]]('{ null.asInstanceOf[b] })
          s"Nested(${prod(n.p)}, x => ${shape(inner)})"
  private def prod(p: Producer[?]): String = p match
    case p: ForP[?, ?]    => s"For<${p.label}>"
    case u: UnfoldP[?, ?] => s"Unfold[${u.card}${if u.suspends then ",suspends" else ""}]<${u.label}>"
