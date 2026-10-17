package iofuse

import zio.{Executor, FiberRef, Promise, Queue, Ref, Runtime, Unsafe, ZIO, ZLayer, durationInt}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/** The ZIO side of [[RuntimeSemanticsSuite]]: interruption and its masks, finalizers, FiberRefs,
 *  and shared state. The concurrent tests run many times (IOFUSE_STRESS multiplies the counts). */
class ZioRuntimeSemanticsSuite extends munit.FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(10, "min")
  private val stress = sys.env.get("IOFUSE_STRESS").map(_.toInt).getOrElse(1)

  def repeated(name: String, times: Int)(body: => Any)(using munit.Location): Unit =
    test(s"$name (x${times * stress})") { for _ <- 1 to times * stress do body }

  inline def pick[E, A](fuse: Boolean)(inline z: ZIO[Any, E, A]): ZIO[Any, E, A] = if fuse then IOFuse.fused(z) else z
  def unsafeRun[E, A](z: ZIO[Any, E, A]): A =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(z.timeoutFail(new RuntimeException("test timeout"))(30.seconds)).getOrThrowFiberFailure())

  def endless(c: AtomicLong): ZIO[Any, Nothing, Unit] = IOFuse.fused {
    def loop(i: Long): ZIO[Any, Nothing, Unit] = ZIO.succeed(c.incrementAndGet()).flatMap(_ => loop(i + 1))
    loop(0)
  }

  repeated("interrupt: an endless synchronous loop stops, and ensuring runs", 10) {
    val (count, finalized) = (AtomicLong(), AtomicBoolean())
    val (a, b) = unsafeRun(
      for
        f <- endless(count).ensuring(ZIO.succeed(finalized.set(true))).fork
        _ <- ZIO.sleep(50.millis)
        _ <- f.interrupt
        a <- ZIO.succeed(count.get)
        _ <- ZIO.sleep(50.millis)
        b <- ZIO.succeed(count.get)
      yield (a, b))
    assert(a > 0)
    assertEquals(b, a)
    assert(finalized.get)
  }

  repeated("fairness: two endless loops on one thread both progress and can be interrupted", 5) {
    val pool = Executors.newSingleThreadExecutor()
    val (a, b) = (AtomicLong(), AtomicLong())
    try
      val r = unsafeRun(
        endless(a).race(endless(b)).timeout(300.millis).provideLayer(Runtime.setExecutor(Executor.fromJavaExecutor(pool))))
      assertEquals(r, None)
      assert(a.get > 0 && b.get > 0, (a.get, b.get))
    finally pool.shutdown()
  }

  repeated("uninterruptible: a fused loop inside the mask runs to the end", 10) {
    val n = 200000
    val r = unsafeRun(
      for
        ref     <- Ref.make(0)
        started <- Promise.make[Nothing, Unit]
        f <- (started.succeed(()) *> IOFuse.fused {
               def loop(i: Int): ZIO[Any, Nothing, Unit] = if i == n then ZIO.unit else ref.update(_ + 1).flatMap(_ => loop(i + 1))
               loop(0)
             }).uninterruptible.fork
        _ <- started.await
        _ <- f.interrupt
        r <- ref.get
      yield r)
    assertEquals(r, n)
  }

  repeated("FiberRef: reads and updates inside a fused loop; a child's updates stay in the child", 10) {
    def prog(fuse: Boolean): ZIO[Any, Nothing, (Int, Int)] = ZIO.scoped {
      FiberRef.make(0).flatMap { local =>
        val body: ZIO[Any, Nothing, Int] =
          if fuse then IOFuse.fused {
            def loop(i: Int, acc: Int): ZIO[Any, Nothing, Int] =
              if i == 5000 then local.get.map(_ + acc)
              else local.update(_ + 1) *> ZIO.succeed(i % 3).flatMap(x => loop(i + 1, acc + x))
            loop(0, 0)
          }
          else
            def loop(i: Int, acc: Int): ZIO[Any, Nothing, Int] =
              if i == 5000 then local.get.map(_ + acc)
              else local.update(_ + 1) *> ZIO.succeed(i % 3).flatMap(x => loop(i + 1, acc + x))
            loop(0, 0)
        for
          child <- (local.set(1000) *> body).fork.flatMap(_.await).flatMap(e => ZIO.succeed(e.getOrElse(_ => -1)))
          mine  <- body
        yield (child, mine)
      }
    }
    assertEquals(unsafeRun(prog(fuse = true)), unsafeRun(prog(fuse = false)))
  }

  repeated("shared Ref: fused loops on many fibers lose no update", 30) {
    val (fibers, n) = (8, 20000)
    val r = unsafeRun(Ref.make(0).flatMap { ref =>
      val one = IOFuse.fused {
        def loop(i: Int): ZIO[Any, Nothing, Unit] = if i == n then ZIO.unit else ref.update(_ + 1).flatMap(_ => loop(i + 1))
        loop(0)
      }
      ZIO.foreachParDiscard(1 to fibers)(_ => one) *> ref.get
    })
    assertEquals(r, fibers * n)
  }

  repeated("Queue: a fused producer and a fused consumer lose and repeat nothing", 30) {
    val n = 20000
    def prog(fuse: Boolean): ZIO[Any, Nothing, (Long, Int)] = Queue.bounded[Int](8).flatMap { q =>
      val produce = pick(fuse) {
        def loop(i: Int): ZIO[Any, Nothing, Unit] = if i == n then q.offer(-1).unit else q.offer(i).flatMap(_ => loop(i + 1))
        loop(0)
      }
      val consume = pick(fuse) {
        def loop(sum: Long, count: Int): ZIO[Any, Nothing, (Long, Int)] =
          q.take.flatMap(x => if x < 0 then ZIO.succeed((sum, count)) else loop(sum + x, count + 1))
        loop(0L, 0)
      }
      produce &> consume
    }
    assertEquals(unsafeRun(prog(fuse = true)), (n.toLong * (n - 1) / 2, n))
    assertEquals(unsafeRun(prog(fuse = false)), (n.toLong * (n - 1) / 2, n))
  }

  repeated("Promise: a fused loop waits for values another fused loop completes", 30) {
    val n = 5000
    def prog(fuse: Boolean): ZIO[Any, Nothing, Long] = Queue.unbounded[(Int, Promise[Nothing, Int])].flatMap { q =>
      val ask = pick(fuse) {
        def loop(i: Int, acc: Long): ZIO[Any, Nothing, Long] =
          if i == n then ZIO.succeed(acc)
          else Promise.make[Nothing, Int].flatMap(p => q.offer((i, p)) *> p.await).flatMap(v => loop(i + 1, acc + v))
        loop(0, 0L)
      }
      val answer = pick(fuse) {
        def loop(i: Int): ZIO[Any, Nothing, Unit] =
          if i == n then ZIO.unit else q.take.flatMap { case (k, p) => p.succeed(k * 2).unit } *> loop(i + 1)
        loop(0)
      }
      ask <& answer
    }
    assertEquals(unsafeRun(prog(fuse = true)), n.toLong * (n - 1))
    assertEquals(unsafeRun(prog(fuse = false)), n.toLong * (n - 1))
  }

  repeated("interrupt while blocked on a Queue: a fused consumer stops and its finalizer runs", 20) {
    val r = unsafeRun(
      for
        q    <- Queue.bounded[Int](1)
        seen <- Ref.make(0)
        done <- Promise.make[Nothing, Unit]
        f <- IOFuse.fused {
               def loop(i: Int): ZIO[Any, Nothing, Unit] = q.take.flatMap(_ => seen.update(_ + 1)) *> loop(i + 1)
               loop(0)
             }.ensuring(done.succeed(())).fork
        _ <- q.offer(1) *> q.offer(2) *> q.offer(3)
        _ <- seen.get.repeatUntil(_ == 3) // the consumer is now blocked in take
        _ <- f.interrupt
        _ <- done.await
        r <- seen.get
      yield r)
    assertEquals(r, 3)
  }
