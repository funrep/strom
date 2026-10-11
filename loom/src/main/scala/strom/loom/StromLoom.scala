package strom.loom

import java.util.concurrent.BlockingQueue
import strom.runtime.{Runner, Segment, SegmentFactory}

/** Runs compiled Ström segments on (virtual) threads, connected by blocking queues.
 *
 *  Blocking on a queue parks a virtual thread cheaply. Between steps the driver polls
 *  `Thread.interrupted()` (cancellation) and calls `Thread.yield()` when a step used up its
 *  work budget, so a long fused loop never pins a carrier thread. */
object StromLoom:
  /** What flows through the queues: chunks of a primitive array, then `End` (or `Failed`). */
  sealed abstract class Msg
  final case class Data(array: AnyRef, offset: Int, length: Int) extends Msg
  case object End extends Msg
  final case class Failed(error: Throwable) extends Msg
  /** A fold's result, sent before `End`. */
  final case class Result(value: Any) extends Msg

  /** Drives `f` on the current thread until done. Inputs and output are queues of [[Msg]].
   *  Throws `InterruptedException` if the thread is interrupted; forwards upstream failures. */
  def run[O](f: SegmentFactory[O], inputs: IndexedSeq[BlockingQueue[Msg]], output: BlockingQueue[Msg],
             maxOut: Int = Segment.DefaultMaxOut, budget: Int = Segment.DefaultBudget): Unit =
    require(inputs.size == f.numInputs, s"segment expects ${f.numInputs} inputs, got ${inputs.size}")
    val runner = Runner(f, maxOut, budget)
    try
      var done = false
      while !done do
        if Thread.interrupted() then throw InterruptedException()
        runner.next() match
          case Runner.NeedInput(p) =>
            inputs(p).take() match
              case Data(a, off, len) => runner.offer(p, a, off, len)
              case End               => runner.endOfInput(p)
              case Failed(e)         => throw e
              case Result(_)         => throw IllegalStateException("unexpected Result on an input queue")
          case Runner.Emit =>
            val src = runner.outArray
            val n = runner.outLength
            val dst = java.lang.reflect.Array.newInstance(src.getClass.getComponentType, n)
            System.arraycopy(src, runner.outOffset, dst, 0, n)
            output.put(Data(dst, 0, n))
          case Runner.Yield => Thread.`yield`()
          case Runner.Done =>
            if runner.isFold then output.put(Result(runner.result))
            output.put(End)
            done = true
    catch
      case e: InterruptedException => throw e
      case e: Throwable =>
        output.put(Failed(e))
        throw e

  /** Starts [[run]] on a new virtual thread. */
  def start[O](f: SegmentFactory[O], inputs: IndexedSeq[BlockingQueue[Msg]], output: BlockingQueue[Msg],
               maxOut: Int = Segment.DefaultMaxOut, budget: Int = Segment.DefaultBudget): Thread =
    Thread.ofVirtual().name("strom-segment").start(() =>
      try run(f, inputs, output, maxOut, budget)
      catch
        case _: InterruptedException => ()
        case _: Throwable => () // already forwarded downstream as `Failed`
    )
