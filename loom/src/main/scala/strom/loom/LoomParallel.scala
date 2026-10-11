package strom.loom

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, LinkedBlockingQueue, Semaphore, ThreadFactory}
import strom.loom.StromLoom.*
import strom.Strom
import strom.runtime.*

/** Executes a [[strom.runtime.ParallelPlan]] on threads. Tiers are connected by bounded queues of [[strom.loom.StromLoom.Msg]].
 *
 *  Coordination (readers, coordinators, routers) runs on virtual threads; the CPU-bound
 *  workers run on `compute` threads: virtual by default, or e.g. a dedicated pool of platform
 *  threads (`Thread.ofPlatform().factory()`) to keep fused loops off the carrier threads that
 *  serve I/O. */
object LoomParallel:
  final case class Config(
      parallelism: Int = Runtime.getRuntime.availableProcessors(),
      queueCapacity: Int = 16,
      maxOut: Int = Segment.DefaultMaxOut,
      budget: Int = Segment.DefaultBudget,
      compute: ThreadFactory = Thread.ofVirtual().name("strom-worker-", 0).factory(),
  ):
    require(parallelism > 0 && queueCapacity > 0)

  /** Running tiers. `join` waits for the last tier, then stops the upstream tiers: a downstream
   *  tier may finish early (e.g. `take`), leaving upstream ones blocked on a full queue.
   *  `cancel` interrupts everything. */
  final class Running(val tiers: Seq[Seq[Thread]]):
    def threads: Seq[Thread] = tiers.flatten
    def join(): Unit =
      tiers.last.foreach(_.join())
      tiers.init.flatten.foreach(_.interrupt())
      threads.foreach(_.join())
    def cancel(): Unit = threads.foreach(_.interrupt())

  /** A parallel plan for a one-input pipeline lambda, e.g.
   *  `LoomParallel.plan((a: Strom[Int]) => a.map(f).filter(p).take(n))`: stateless parts are
   *  replicated, stateful ones run as single instances, an associative fold (`sum`, `count`,
   *  `foldAssociative`) runs as per-chunk partial folds. */
  inline def plan[A, R](inline f: Strom[A] => R): ParallelPlan[Strom.ResultOf[R]] = ${ strom.internal.Macros.parallelOf[R]('f) }

  private val coordination: ThreadFactory = Thread.ofVirtual().name("strom-coord-", 0).factory()

  /** Starts the plan reading `input` and writing `output` (ends with `End`, or `Failed`). */
  def start(plan: ParallelPlan[?], input: BlockingQueue[Msg], output: BlockingQueue[Msg], cfg: Config = Config()): Running =
    var in = input
    val tiers = plan.tiers.zipWithIndex.map { (tier, i) =>
      val out = if i == plan.tiers.size - 1 then output else ArrayBlockingQueue[Msg](cfg.queueCapacity)
      val ts = startTier(tier, in, out, cfg)
      in = out
      ts
    }
    Running(tiers)

  /** Per worker: honour interrupts at every budget boundary, give way once per time slice. */
  private def yielder(): () => Unit =
    val slice = TimeSlice(Segment.DefaultSliceNanos)
    () =>
      if Thread.interrupted() then throw InterruptedException()
      if slice.due() then Thread.`yield`()

  private def spawn(f: ThreadFactory)(body: => Unit): Thread =
    val t = f.newThread(() =>
      try body
      catch case _: InterruptedException => ()
    )
    t.start()
    t

  private def chunkOf(d: Data) = ArrayChunk(d.array, d.offset, d.length)

  private def startTier(tier: Tier, in: BlockingQueue[Msg], out: BlockingQueue[Msg], cfg: Config): Seq[Thread] = tier match
    case Tier.Single(f) =>
      Seq(spawn(cfg.compute)(StromLoom.run(f, Vector(in), out, cfg.maxOut, cfg.budget)))
    case Tier.Replicated(f) =>
      ordered(in, out, cfg, f, (seg, c, y) =>
          // The outputs before a failure are delivered; the failure itself when its turn comes.
          Chunks.processUntilFailure(seg, c, y) match
            case Processed(chunk, null) => chunk
            case Processed(chunk, e)    => Thrown(e, chunk),
        (acc, r) => { val c = r.asInstanceOf[ArrayChunk]; if c.length > 0 then out.put(Data(c.array, c.offset, c.length)); acc },
        _ => ())
    case Tier.PartialFold(f, combine) =>
      var any = false
      ordered(in, out, cfg, f, (seg, c, y) => Chunks.partialFold(seg, c, y),
        (acc, r) => { val v = if any then combine(acc, r) else r; any = true; v },
        acc => {
          val res = if any then acc else Chunks.end(f.create(cfg.maxOut, cfg.budget))._2
          out.put(Result(res))
        })

  // ------------------------------------------------------------------ ordered (replicated / partial fold)

  private sealed trait Event
  private final case class Input(chunk: ArrayChunk) extends Event
  private case object InputEnd extends Event
  private final case class Done(seq: Long, value: Any) extends Event
  private final case class Fail(e: Throwable) extends Event
  private final class Box(val value: Any)
  /** A chunk whose processing failed: reported only when its turn comes in input order. */
  private final class Thrown(val error: Throwable, val before: ArrayChunk = null)

  /** Chunks go round-robin to N workers; results are released in input order through a
   *  [[ReorderBuffer]] whose window is enforced by a semaphore on the reader. */
  private def ordered(in: BlockingQueue[Msg], out: BlockingQueue[Msg], cfg: Config, f: SegmentFactory[?],
                      work: (Segment, ArrayChunk, () => Unit) => Any, deliver: (Any, Any) => Any, finish: Any => Unit): Seq[Thread] =
    val n = cfg.parallelism
    val window = 2 * n + cfg.queueCapacity
    val permits = Semaphore(window)
    val events = LinkedBlockingQueue[Event]()
    val jobs = Vector.fill(n)(LinkedBlockingQueue[(Long, ArrayChunk)]())
    val workers = (0 until n).map { w =>
      spawn(cfg.compute) {
        var seg = f.create(cfg.maxOut, cfg.budget)
        val onYield = yielder()
        while true do
          val (seq, chunk) = jobs(w).take()
          if seq < 0 then throw InterruptedException() // stop
          try
            events.put(Done(seq, work(seg, chunk, onYield)))
            if seg.isPoisoned then seg = f.create(cfg.maxOut, cfg.budget) // failed (result held)
          catch
            case e: InterruptedException => throw e
            case e: Throwable =>
              // Chunks are processed ahead of demand: the failure is held until its turn, so work
              // that is never needed (past a downstream `take`) can't fail the plan.
              events.put(Done(seq, Thrown(e)))
              seg = f.create(cfg.maxOut, cfg.budget) // the failed instance is poisoned
      }
    }
    val reader = spawn(coordination) {
      var reading = true
      while reading do
        permits.acquire()
        in.take() match
          case d: Data   => events.put(Input(chunkOf(d)))
          case End       => events.put(InputEnd); reading = false
          case Failed(e) => events.put(Fail(e)); reading = false
          case Result(_) => events.put(Fail(IllegalStateException("unexpected Result"))); reading = false
    }
    val coordinator = spawn(coordination) {
      val reorder = ReorderBuffer[Box](window)
      val inFlight = InFlight(window)
      var seq = 0L
      var ended = false
      var acc: Any = null
      var running = true
      def stop(): Unit =
        jobs.foreach(_.put((-1L, null)))
        running = false
      def fail(e: Throwable): Unit =
        out.put(Failed(e))
        stop()
        reader.interrupt()
      /** Delivers results in input order; a held failure is reported when its turn comes. */
      def drain(): Unit =
        var b = reorder.poll()
        while b != null && running do
          b.value match
            case t: Thrown =>
              if (t.before ne null) && t.before.length > 0 then acc = deliver(acc, t.before)
              fail(t.error)
            case v =>
              acc = deliver(acc, v)
              inFlight.release()
              permits.release()
              b = reorder.poll()
      while running do
        events.take() match
          case Input(chunk) =>
            if !inFlight.tryAcquire() then throw IllegalStateException("in-flight window exceeded")
            jobs((seq % n).toInt).put((seq, chunk))
            seq += 1
          case InputEnd => ended = true
          case Done(s, v) =>
            reorder.put(s, Box(v))
            drain()
          case Fail(e) =>
            // An upstream failure takes its place in the order: earlier chunks are delivered
            // first, and a plan that stops before reaching it never reports it.
            if inFlight.tryAcquire() then
              reorder.put(seq, Box(Thrown(e)))
              seq += 1
              ended = true
              drain()
            else fail(e)
        if running && ended && reorder.expected == seq then
          finish(acc)
          out.put(End)
          stop()
    }
    workers :+ reader :+ coordinator
