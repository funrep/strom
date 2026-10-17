package strom.effect

import cats.effect.IO
import zio.ZIO
import scala.collection.mutable.ListBuffer

/** Helpers for the generated fuzz programs (see project/EffectFuzzGen.scala). */
object FuzzSupport:
  final class Boom(msg: String) extends RuntimeException(msg)
  /** A synchronous step: logs and returns v. */
  def step(log: ListBuffer[String], t: String, v: Int): Int = { log += s"$t $v"; v }
  /** A synchronous step that throws for some values. */
  def boom(log: ListBuffer[String], t: String, v: Int): Int =
    log += s"$t $v"
    if (v & 3) == 1 then throw Boom(t) else v
  /** An effect expression whose evaluation is visible: logs when the effect is built. */
  def bld(log: ListBuffer[String], t: String, io: IO[Int]): IO[Int] = { log += s"b$t"; io }
  def bldZ(log: ListBuffer[String], t: String, z: Z): Z = { log += s"b$t"; z }
  /** An effect expression that throws for some values while it is built. */
  def bldx(log: ListBuffer[String], t: String, v: Int): IO[Int] =
    log += s"x$t $v"
    if (v & 3) == 1 then throw Boom(t) else IO.pure(v)
  def bldxZ(log: ListBuffer[String], t: String, v: Int): Z =
    log += s"x$t $v"
    if (v & 3) == 1 then throw Boom(t) else ZIO.succeed(v)
  /** A function value whose evaluation is visible. */
  def fnv(log: ListBuffer[String], t: String): Int => IO[Int] = { log += s"f$t"; x => IO { log += s"a$t $x"; x + 1 } }
  def fnvZ(log: ListBuffer[String], t: String): Int => Z = { log += s"f$t"; x => ZIO.succeed { log += s"a$t $x"; x + 1 } }
  /** A log of its own for a concurrent branch, kept with its parent's. */
  private val children = new java.util.IdentityHashMap[ListBuffer[String], ListBuffer[(String, ListBuffer[String])]]
  def sub(parent: ListBuffer[String], t: String): ListBuffer[String] = children.synchronized {
    val child = ListBuffer.empty[String]
    var cs = children.get(parent)
    if cs eq null then { cs = ListBuffer.empty; children.put(parent, cs) }
    cs += ((t, child))
    child
  }
  /** A log with its branches' logs: in tag order (branches start in any order), and in the order
   *  they started for one tag. Read it when the program is done. */
  def flat(log: ListBuffer[String]): List[String] = children.synchronized {
    val cs = Option(children.get(log)).map(_.toList).getOrElse(Nil).sortBy(_._1)
    log.toList ++ cs.flatMap((t, c) => s"[$t" :: flat(c) ::: List("]"))
  }
  def reset(): Unit = children.synchronized(children.clear())
  /** The ZIO programs' type: typed failures are Strings. */
  type Z = ZIO[Any, String, Int]
  /** Opaque calls (the macro can't see into them). */
  def opqZ(log: ListBuffer[String], t: String, v: Int): ZIO[Any, Nothing, Int] = ZIO.succeed { log += s"o$t $v"; v * 3 + 1 }
  def opq(log: ListBuffer[String], t: String, v: Int): IO[Int] =
    IO { log += s"o$t $v"; v * 3 + 1 }
