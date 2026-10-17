package iofuse

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.collection.mutable.ListBuffer

/** With frames on, loops and blocks that suspend run as a state machine on a frame: the values
 *  that live across a suspension are in the frame's slots. Compared with the plain code. */
class FrameSuite extends munit.FunSuite:
  final class Boom(msg: String) extends RuntimeException(msg)
  type Log = ListBuffer[String]

  def outcome[A](io: IO[A]): Either[String, A] =
    io.attempt.unsafeRunSync().left.map(e => s"${e.getClass.getSimpleName}: ${e.getMessage}")

  inline def same[A](inline prog: Log => IO[A])(using munit.Location): A =
    val (pl, fl) = (ListBuffer.empty[String], ListBuffer.empty[String])
    val p = outcome(prog(pl))
    val f = outcome(IOFuse.fusedWith(IOFuse.YieldEvery, frames = true)(prog(fl)))
    assertEquals(f, p)
    assertEquals(fl.toList, pl.toList)
    p.getOrElse(null.asInstanceOf[A])

  def op(log: Log, s: String, v: Int): IO[Int] = IO { log += s"$s $v"; v + 1 }
  def fail(log: Log, s: String): IO[Int] = IO { log += s; throw Boom(s) }

  test("more values than the frame has fields, of every kind") {
    val r = same(log =>
      def loop(i: Int, a: Int, b: Int, c: Int, d: Int, e: Int, l1: Long, l2: Long, l3: Long,
               s: String, xs: List[Int], flag: Boolean, dbl: Double, ch: Char, u: Unit, o: Option[Int]): IO[String] =
        if i == 0 then IO.pure(s"$a $b $c $d $e $l1 $l2 $l3 ${s.length} $xs $flag $dbl $ch $u $o")
        else
          for
            x <- op(log, "x", a + i)
            y <- op(log, "y", b + x)
            t = (x, y)
            z <- op(log, "z", c + t._1)
            v <- IO(d + e)
            w <- op(log, "w", v)
            r <- loop(i - 1, b, c, d, e, x + w, l2 + 1, l3 * 2 + y, l1 ^ z, s + ch, x :: xs.take(3), !flag, dbl / 2 + y,
                      (ch + 1).toChar, u, o.map(_ + z).filter(_ % 7 != 0).orElse(Some(i)))
          yield r
      loop(1500, 1, 2, 3, 4, 5, 6L, 7L, 8L, "s", Nil, true, 1.5, 'a', (), None))
    assert(r.split(" ").length >= 15, r)
  }

  test("a block that is not a loop: one frame for its suspensions") {
    for n <- 0 to 3 do
      same(log =>
        for
          a <- op(log, "a", n)
          b <- op(log, "b", a * 2)
          c <- if b % 3 == 0 then op(log, "c3", b) else IO(b - 1)
          d <- op(log, "d", a + c).map(x => (x, x * 2))
          e <- d match
            case (p, q) if p % 2 == 0 => op(log, "even", p + q)
            case (p, q) => op(log, "odd", q).flatMap(v => op(log, "odd2", v + p + a))
          f <- (if e % 2 == 0 then fail(log, "boom") else op(log, "f", e)).handleErrorWith(t => op(log, t.getMessage, b))
          g <- op(log, "g", f + c + d._1)
        yield (a, b, c, d, e, f, g))
    assert(IOFuse.showWith(IOFuse.YieldEvery, frames = true) {
      val log = ListBuffer.empty[String]
      op(log, "a", 1).map(_ + 1).flatMap(a => op(log, "b", a).flatMap(b => op(log, "c", b + a).map(_ + a)))
    }.contains("new Frame"))
  }

  test("each run has its own frame") {
    val log = java.util.concurrent.ConcurrentLinkedQueue[String]()
    def op2(v: Int): IO[Int] = IO.cede *> IO { log.add("x"); v + 1 }
    val block = IOFuse.fusedWith(IOFuse.YieldEvery, frames = true)(op2(1).flatMap(a => op2(a).flatMap(b => op2(a + b).flatMap(c => op2(a + b + c)))))
    val loop = IOFuse.fusedWith(IOFuse.YieldEvery, frames = true) {
      def go(i: Int, acc: Int): IO[Int] = if i == 0 then IO.pure(acc) else op2(acc).flatMap(x => op2(x + i)).flatMap(y => go(i - 1, y))
      go(200, 0)
    }
    val (a, b) = (block.unsafeRunSync(), loop.unsafeRunSync())
    val rs = IO.parReplicateAN(8)(64, IO.both(block, loop)).unsafeRunSync()
    assert(rs.forall(_ == (a, b)), rs.distinct)
  }

  test("an error in one state; a handler around several states") {
    same(log =>
      def loop(i: Int, acc: Int): IO[Int] =
        if i == 0 then IO.pure(acc)
        else
          (for
            x <- op(log, "x", i)
            y <- if x % 5 == 0 then fail(log, s"bad $i") else op(log, "y", x + acc)
            z <- op(log, "z", y)
          yield z).handleErrorWith(e => IO(log += e.getMessage).as(-i)).flatMap(v => loop(i - 1, v & 0xffff))
      loop(300, 0))
    same(log =>
      def loop(i: Int): IO[Int] = op(log, "x", i).flatMap(x => if x == 7 then fail(log, "stop") else op(log, "y", x)).flatMap(_ => loop(i + 1))
      loop(0))
  }

  test("a suspended frame does not keep a value that no later state reads") {
    import cats.effect.Deferred
    import java.lang.ref.WeakReference
    import scala.concurrent.duration.*
    def make(): IO[(Array[Byte], WeakReference[Array[Byte]])] =
      IO { val a = new Array[Byte](1 << 20); (a, WeakReference(a)) }
    def one: IO[Int] = IO(1)
    val t = for
      d <- Deferred[IO, Unit]
      w <- Deferred[IO, WeakReference[Array[Byte]]]
      fiber <- IOFuse.fusedWith(IOFuse.YieldEvery, frames = true) {
        for
          p <- make()
          _ <- w.complete(p._2)
          n <- one
          k = p._1.length + n // the last use of the array
          _ <- d.get           // suspended here, for as long as the test likes
          m <- one
        yield k + m
      }.start
      weak <- w.get
      // Wait until the fiber is suspended on d, then until a GC has cleared the array.
      gone <- (IO.sleep(20.millis) *> IO { System.gc(); weak.get eq null }).iterateUntil(identity).timeout(5.seconds).attempt
      _ <- d.complete(())
      r <- fiber.joinWithNever
    yield (gone, r)
    val (gone, r) = t.unsafeRunSync()
    assertEquals(r, (1 << 20) + 2)
    assert(gone.isRight, "the frame kept the array alive while suspended")
  }
