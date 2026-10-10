package strom.runtime

import strom.runtime.Segment.Status

/** Sans-I/O driver for a [[Segment]]: it never blocks, never schedules and never performs I/O.
 *  An adapter (fs2, ZIO, threads, …) repeatedly calls [[next]] and reacts to the signal:
 *
 *   - [[Runner.NeedInput]]`(port)`: call [[offer]] (or [[endOfInput]]) for that port.
 *   - [[Runner.Emit]]: read `outArray(outOffset until outOffset + outLength)` before the next call
 *     (the buffer is reused afterwards; copy it to keep it).
 *   - [[Runner.Yield]]: the work budget of one step was used up; give the scheduler a chance
 *     to run something else (cede / yieldNow / Thread.yield), then call `next()` again.
 *   - [[Runner.Done]]: the pipeline finished; for a fold, [[result]] holds the value.
 *
 *  Signals are preallocated, so driving a runner allocates nothing.
 */
final class Runner[Out](val segment: Segment):
  import Runner.*

  private var pending: Int = Status.Continue

  def offer(port: Int, array: AnyRef, offset: Int, length: Int): Unit = segment.offer(port, array, offset, length)
  def endOfInput(port: Int): Unit = segment.endOfInput(port)

  def next(): Signal =
    val st =
      if pending != Status.Continue then
        val p = pending
        pending = Status.Continue
        p
      else
        val s = segment.step()
        if segment.outLen > 0 then
          // A full buffer needs nothing from the driver once drained: just step again.
          pending = if s == Status.Emit then Status.Continue else s
          return Emit
        s
    st match
      case Status.Emit  => throw IllegalStateException("segment reported Emit without output")
      case Status.Yield => Yield
      case Status.Done  => Done
      case port         => needInput(port)

  def outArray: AnyRef = segment.out
  def outOffset: Int = 0
  def outLength: Int = segment.outLen
  def result: Out = segment.result.asInstanceOf[Out]
  def isFold: Boolean = segment.isFold

object Runner:
  def apply[Out](f: SegmentFactory[Out], maxOut: Int = 1024, budget: Int = 4096): Runner[Out] =
    new Runner[Out](f.create(maxOut, budget))

  sealed abstract class Signal
  final case class NeedInput(port: Int) extends Signal
  case object Emit extends Signal
  case object Yield extends Signal
  case object Done extends Signal

  private val needs = Array.tabulate(Segment.MaxPorts)(NeedInput(_))
  def needInput(port: Int): NeedInput = needs(port)

/** Stable key → partition assignment (for key-partitioned stateful stages). */
object KeyPartitioner:
  /** Partition in `[0, partitions)` for a key hash; uses a murmur3 finaliser so that
   *  consecutive or poorly distributed hash codes still spread evenly. */
  def partition(hash: Int, partitions: Int): Int =
    require(partitions > 0)
    var h = hash
    h ^= h >>> 16; h *= 0x85ebca6b
    h ^= h >>> 13; h *= 0xc2b2ae35
    h ^= h >>> 16
    ((h & 0x7fffffffL) % partitions).toInt

  def partitionOf(key: Any, partitions: Int): Int = partition(key.##, partitions)

/** Reorders results that complete out of order (e.g. chunks processed by N stateless workers)
 *  back into sequence order. Not thread-safe: own it from one thread / fiber. Bounded: at most
 *  `capacity` sequence numbers may be outstanding past the next one to deliver. */
final class ReorderBuffer[A <: AnyRef](val capacity: Int):
  require(capacity > 0)
  private val slots = new Array[AnyRef](capacity)
  private var nextSeq = 0L

  /** Sequence number of the next item to be released. */
  def expected: Long = nextSeq

  /** Whether `seq` can be accepted now (it lies within the window). */
  def accepts(seq: Long): Boolean = seq >= nextSeq && seq < nextSeq + capacity

  def put(seq: Long, item: A): Unit =
    if !accepts(seq) then throw IllegalArgumentException(s"sequence $seq outside window [$nextSeq, ${nextSeq + capacity})")
    val i = (seq % capacity).toInt
    if slots(i) ne null then throw IllegalStateException(s"sequence $seq already present")
    slots(i) = item

  /** The next item in order, or null if it has not arrived yet. */
  def poll(): A =
    val i = (nextSeq % capacity).toInt
    val item = slots(i)
    if item ne null then
      slots(i) = null
      nextSeq += 1
    item.asInstanceOf[A]

/** Counts items in flight between tiers against a fixed bound (back-pressure accounting).
 *  Pure bookkeeping: the caller decides how to wait when `tryAcquire` fails. Not thread-safe. */
final class InFlight(val limit: Int):
  require(limit > 0)
  private var used = 0
  def inFlight: Int = used
  def available: Int = limit - used
  def tryAcquire(n: Int = 1): Boolean =
    if n <= limit - used then { used += n; true } else false
  def release(n: Int = 1): Unit =
    if n > used then throw IllegalStateException(s"releasing $n with only $used in flight")
    used -= n
