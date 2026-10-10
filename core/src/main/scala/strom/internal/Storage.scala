package strom.internal

import scala.quoted.*
import strom.runtime.Slots

/** A staged mutable variable. `get` builds a fresh read each time it is called. */
abstract class Var[T](val slotName: String)(using val tpe: Type[T]):
  def get: Expr[T]
  def :=(v: Expr[T]): Expr[Unit]

/** Where staged state lives. */
trait Storage:
  def alloc[T: Type](hint: String)(using Quotes): Var[T]
  /** Resets allocation (used when the generator re-runs with a different splitting decision). */
  def reset(): Unit

object Storage:
  def zero[T: Type](using Quotes): Expr[T] =
    import quotes.reflect.*
    val t = TypeRepr.of[T].dealias.widen
    val lit: Term =
      if t =:= TypeRepr.of[Int] then Literal(IntConstant(0))
      else if t =:= TypeRepr.of[Long] then Literal(LongConstant(0L))
      else if t =:= TypeRepr.of[Double] then Literal(DoubleConstant(0.0))
      else if t =:= TypeRepr.of[Float] then Literal(FloatConstant(0f))
      else if t =:= TypeRepr.of[Boolean] then Literal(BooleanConstant(false))
      else if t =:= TypeRepr.of[Byte] then Literal(ByteConstant(0))
      else if t =:= TypeRepr.of[Short] then Literal(ShortConstant(0))
      else if t =:= TypeRepr.of[Char] then Literal(CharConstant(0))
      else if t =:= TypeRepr.of[Unit] then Literal(UnitConstant())
      else '{ null.asInstanceOf[T] }.asTerm
    lit.asExprOf[T]

/** Mutable locals hoisted to the top of the generated method (direct mode). */
final class LocalStorage(using q0: Quotes)(owner: q0.reflect.Symbol) extends Storage:
  import q0.reflect.*
  private var defs = Vector.empty[(Symbol, Term)]
  private var counter = 0

  def reset(): Unit = { defs = Vector.empty; counter = 0 }

  def alloc[T: Type](hint: String)(using Quotes): Var[T] =
    counter += 1
    val sym = Symbol.newVal(owner, s"$hint$counter", TypeRepr.of[T], Flags.Mutable, Symbol.noSymbol)
    defs :+= (sym -> Storage.zero[T](using summon[Type[T]], q0).asTerm)
    new Var[T](sym.name):
      def get: Expr[T] = Ref(sym).asExprOf[T]
      def :=(v: Expr[T]): Expr[Unit] = Assign(Ref(sym), v.asTerm).asExprOf[Unit]

  /** Wraps `body` with the declarations of all variables allocated so far. */
  def wrap[R: Type](body: Expr[R]): Expr[R] =
    Block(defs.toList.map((s, z) => ValDef(s, Some(z))), body.asTerm).asExprOf[R]

/** Variables mapped onto the primitive slot fields of [[Slots]] (resumable segments). */
final class FieldStorage(using q: Quotes)(self: Expr[Slots]) extends Storage:
  import q.reflect.*
  private val used = collection.mutable.Map.empty[Char, Int].withDefaultValue(0)
  /** Slot name → what the generator allocated it for (for `Strom.showCode`). */
  val legend = collection.mutable.ArrayBuffer.empty[(String, String)]
  private var hint = ""

  def reset(): Unit = { used.clear(); legend.clear() }

  private def slot(kind: Char): String =
    val n = used(kind)
    if n >= Slots.capacity(kind) then
      throw StromError(s"pipeline needs more than ${Slots.capacity(kind)} '$kind' state slots")
    used(kind) = n + 1
    legend += (s"$kind$n" -> hint)
    s"$kind$n"

  private def field(name: String): Term = Select.unique(self.asTerm, name)

  def alloc[T: Type](hint: String)(using Quotes): Var[T] =
    this.hint = hint
    val t = TypeRepr.of[T].dealias.widen
    def plain(kind: Char): Var[T] =
      val nm = slot(kind)
      new Var[T](nm):
        def get: Expr[T] = field(nm).asExprOf[T]
        def :=(v: Expr[T]): Expr[Unit] = Assign(field(nm), v.asTerm).asExprOf[Unit]
    // Narrow integral types share Int slots.
    def narrowed(conv: Expr[Int] => Expr[T]): Var[T] =
      val nm = slot('i')
      new Var[T](nm):
        def get: Expr[T] = conv(field(nm).asExprOf[Int])
        def :=(v: Expr[T]): Expr[Unit] = Assign(field(nm), Select.unique(v.asTerm, "toInt")).asExprOf[Unit]
    if t =:= TypeRepr.of[Int] then plain('i')
    else if t =:= TypeRepr.of[Long] then plain('j')
    else if t =:= TypeRepr.of[Double] then plain('d')
    else if t =:= TypeRepr.of[Float] then plain('f')
    else if t =:= TypeRepr.of[Boolean] then plain('z')
    else if t =:= TypeRepr.of[Byte] then narrowed(e => '{ $e.toByte }.asExprOf[T])
    else if t =:= TypeRepr.of[Short] then narrowed(e => '{ $e.toShort }.asExprOf[T])
    else if t =:= TypeRepr.of[Char] then narrowed(e => '{ $e.toChar }.asExprOf[T])
    else
      val nm = slot('r')
      new Var[T](nm):
        def get: Expr[T] = '{ ${ field(nm).asExprOf[AnyRef] }.asInstanceOf[T] }
        def :=(v: Expr[T]): Expr[Unit] = Assign(field(nm), '{ $v.asInstanceOf[AnyRef] }.asTerm).asExprOf[Unit]

final class StromError(msg: String) extends Exception(msg)
