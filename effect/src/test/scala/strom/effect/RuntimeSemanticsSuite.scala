package strom.effect

import cats.effect.{Deferred, IO, IOLocal, Ref}
import cats.effect.std.Queue
import cats.effect.unsafe.IORuntime
import cats.effect.unsafe.implicits.global
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** What the runtime owns must survive fusion: cancelation and its masks, finalizers, fiber-local
 *  state, and shared state updated from many fibers.
 *
 *  The concurrent tests run many times: a race shows up in some runs only. STROM_STRESS
 *  multiplies the counts (default 1). */
class RuntimeSemanticsSuite extends munit.FunSuite:
  final class Boom(msg: String) extends RuntimeException(msg)
  override val munitTimeout = 10.minutes
  private val stress = sys.env.get("STROM_STRESS").map(_.toInt).getOrElse(1)

  def repeated(name: String, times: Int)(body: => Any)(using munit.Location): Unit =
    test(s"$name (x${times * stress})") { for _ <- 1 to times * stress do body }

  /** The same code plain or fused. */
  inline def pick[A](fuse: Boolean)(inline io: IO[A]): IO[A] = if fuse then StromEffect.fused(io) else io

  def endless(c: AtomicLong): IO[Unit] = StromEffect.fused {
    def loop(i: Long): IO[Unit] = IO(c.incrementAndGet()).flatMap(_ => loop(i + 1))
    loop(0)
  }

  repeated("cancel: an endless synchronous loop stops, and onCancel / guarantee run", 10) {
    val (count, canceled, finalized) = (AtomicLong(), AtomicBoolean(), AtomicBoolean())
    val prog =
      for
        f <- endless(count).onCancel(IO(canceled.set(true))).guarantee(IO(finalized.set(true))).start
        _ <- IO.sleep(50.millis)
        _ <- f.cancel
        a <- IO(count.get)
        _ <- IO.sleep(50.millis)
        b <- IO(count.get)
      yield (a, b)
    val (a, b) = prog.timeout(10.seconds).unsafeRunSync()
    assert(a > 0)
    assertEquals(b, a) // nothing ran after cancel returned
    assert(canceled.get && finalized.get)
  }

  repeated("cancel latency: at most one cede interval of iterations after the cancel is requested", 10) {
    // One compute thread: the canceling fiber can only run when the loop cedes.
    val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
    val rt = IORuntime.builder().setCompute(ec, () => ec.shutdown()).build()
    val count = AtomicLong()
    val atCancel = AtomicLong(-1)
    try
      val prog =
        for
          f <- endless(count).start
          _ <- IO.sleep(30.millis)
          _ <- IO(atCancel.set(count.get))
          _ <- f.cancel
        yield count.get - atCancel.get
      val extra = prog.timeout(10.seconds).unsafeRunSync()(using rt)
      assert(extra <= 2L * StromEffect.YieldEvery, extra)
    finally rt.shutdown()
  }

  repeated("uncancelable: a fused loop inside the mask runs to the end", 10) {
    val n = 200000
    val prog =
      for
        ref     <- Ref[IO].of(0)
        started <- Deferred[IO, Unit]
        f <- IO.uncancelable { _ =>
               started.complete(()) *> StromEffect.fused {
                 def loop(i: Int): IO[Unit] = if i == n then IO.unit else ref.update(_ + 1).flatMap(_ => loop(i + 1))
                 loop(0)
               }
             }.start
        _ <- started.get
        _ <- f.cancel // returns only when the masked region is done
        r <- ref.get
      yield r
    assertEquals(prog.timeout(30.seconds).unsafeRunSync(), n)
  }

  test("guarantee runs when fused code raises; the error is unchanged") {
    val finalized = AtomicBoolean()
    val r = StromEffect.fused(IO(1).flatMap(x => IO.raiseError[Int](Boom(s"x$x")))).guarantee(IO(finalized.set(true))).attempt.unsafeRunSync()
    assertEquals(r.left.map(_.getMessage), Left("x1"))
    assert(finalized.get)
  }

  repeated("IOLocal: reads and updates inside a fused loop, across cedes", 10) {
    def prog(fuse: Boolean): IO[(Int, Int)] = IOLocal(0).flatMap { local =>
      val body =
        if fuse then StromEffect.fused {
          def loop(i: Int, acc: Int): IO[Int] =
            if i == 5000 then local.get.map(_ + acc)
            else local.update(_ + 1) *> IO(i % 3).flatMap(x => loop(i + 1, acc + x))
          loop(0, 0)
        }
        else
          def loop(i: Int, acc: Int): IO[Int] =
            if i == 5000 then local.get.map(_ + acc)
            else local.update(_ + 1) *> IO(i % 3).flatMap(x => loop(i + 1, acc + x))
          loop(0, 0)
      // A child fiber's updates must not leak into the parent.
      for
        child <- (local.set(1000) *> body).start.flatMap(_.joinWithNever)
        mine  <- body
      yield (child, mine)
    }
    assertEquals(prog(fuse = true).unsafeRunSync(), prog(fuse = false).unsafeRunSync())
  }

  repeated("shared Ref: fused loops on many fibers lose no update", 30) {
    val (fibers, n) = (8, 20000)
    val prog = Ref[IO].of(0).flatMap { ref =>
      val one = StromEffect.fused {
        def loop(i: Int): IO[Unit] = if i == n then IO.unit else ref.update(_ + 1).flatMap(_ => loop(i + 1))
        loop(0)
      }
      IO.parReplicateAN(fibers)(fibers, one) *> ref.get
    }
    assertEquals(prog.unsafeRunSync(), fibers * n)
  }

  repeated("Queue: a fused producer and a fused consumer lose and repeat nothing", 30) {
    val n = 20000
    def prog(fuse: Boolean): IO[(Long, Int)] = Queue.bounded[IO, Int](8).flatMap { q =>
      val produce = pick(fuse) {
        def loop(i: Int): IO[Unit] = if i == n then q.offer(-1) else q.offer(i).flatMap(_ => loop(i + 1))
        loop(0)
      }
      val consume = pick(fuse) {
        def loop(sum: Long, count: Int): IO[(Long, Int)] =
          q.take.flatMap(x => if x < 0 then IO.pure((sum, count)) else loop(sum + x, count + 1))
        loop(0L, 0)
      }
      IO.both(produce, consume).map(_._2)
    }
    assertEquals(prog(fuse = true).timeout(30.seconds).unsafeRunSync(), (n.toLong * (n - 1) / 2, n))
    assertEquals(prog(fuse = false).timeout(30.seconds).unsafeRunSync(), (n.toLong * (n - 1) / 2, n))
  }

  repeated("Deferred: a fused loop waits for values another fused loop completes", 30) {
    val n = 5000
    def prog(fuse: Boolean): IO[Long] = Queue.unbounded[IO, (Int, Deferred[IO, Int])].flatMap { q =>
      val ask = pick(fuse) {
        def loop(i: Int, acc: Long): IO[Long] =
          if i == n then IO.pure(acc)
          else Deferred[IO, Int].flatMap(d => q.offer((i, d)) *> d.get).flatMap(v => loop(i + 1, acc + v))
        loop(0, 0L)
      }
      val answer = pick(fuse) {
        def loop(i: Int): IO[Unit] =
          if i == n then IO.unit else q.take.flatMap { case (k, d) => d.complete(k * 2).void } >> loop(i + 1)
        loop(0)
      }
      IO.both(ask, answer).map(_._1)
    }
    assertEquals(prog(fuse = true).timeout(30.seconds).unsafeRunSync(), n.toLong * (n - 1))
    assertEquals(prog(fuse = false).timeout(30.seconds).unsafeRunSync(), n.toLong * (n - 1))
  }

  repeated("cancel while blocked on a Queue: a fused consumer stops and its finalizer runs", 20) {
    val prog =
      for
        q    <- Queue.bounded[IO, Int](1)
        seen <- Ref[IO].of(0)
        done <- Deferred[IO, Unit]
        f <- StromEffect.fused {
               def loop(i: Int): IO[Unit] = q.take.flatMap(_ => seen.update(_ + 1)) >> loop(i + 1)
               loop(0)
             }.guarantee(done.complete(()).void).start
        _ <- q.offer(1) *> q.offer(2) *> q.offer(3)
        _ <- seen.get.iterateUntil(_ == 3) // the consumer is now blocked in take
        _ <- f.cancel
        _ <- done.get
        r <- seen.get
      yield r
    assertEquals(prog.timeout(30.seconds).unsafeRunSync(), 3)
  }

  test("errors thrown in fused code carry a JVM stack trace through this file") {
    val e = StromEffect.fused(IO(1).map(x => if x == 1 then throw Boom("here") else x)).attempt.unsafeRunSync().swap.toOption.get
    assert(e.getStackTrace.exists(_.getFileName == "RuntimeSemanticsSuite.scala"), e.getStackTrace.take(5).mkString("\n"))
  }
