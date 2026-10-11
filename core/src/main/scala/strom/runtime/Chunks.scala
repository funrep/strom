package strom.runtime

import strom.runtime.Segment.Status

/** The outputs of a chunk, and the error that stopped it (null if none). */
final case class Processed(chunk: ArrayChunk, error: Throwable)

/** A slice of a primitive array: the unit of work between tiers. */
final case class ArrayChunk(array: AnyRef, offset: Int, length: Int):
  def elements: Iterator[Any] = Iterator.range(offset, offset + length).map(java.lang.reflect.Array.get(array, _))

object ArrayChunk:
  /** A one-element chunk holding `x` (e.g. a fold result). */
  def single(x: Any): ArrayChunk = ArrayChunk(Array[Any](x), 0, 1)

  /** The runner's output buffer copied into a fresh array of the same primitive type. */
  def copyOut(runner: Runner[?]): ArrayChunk =
    val src = runner.outArray
    val n = runner.outLength
    val dst = java.lang.reflect.Array.newInstance(src.getClass.getComponentType, n)
    System.arraycopy(src, runner.outOffset, dst, 0, n)
    ArrayChunk(dst, 0, n)

/** Segments reused across chunks by concurrent tasks (a stateless instance carries no state
 *  between chunks, so any free instance will do). */
final class SegmentPool(f: SegmentFactory[?], maxOut: Int, budget: Int):
  private val free = java.util.concurrent.ConcurrentLinkedQueue[Segment]()
  def use[X](body: Segment => X): X =
    val seg = Option(free.poll()).getOrElse(f.create(maxOut, budget))
    val x = body(seg)
    if !seg.isPoisoned then free.offer(seg) // a poisoned instance is dropped
    x

/** Synchronous, chunk-at-a-time operations on segments, shared by all parallel executors.
 *  They allocate per chunk (the result array), never per element. `onYield` is called when a
 *  step uses up its budget, so a driver can give way (e.g. `Thread.yield()`). */
object Chunks:
  private val NoYield: () => Unit = () => ()

  /** Steps `seg` until it needs input or is done, appending what it emits to `out` (if any).
   *  Returns whether the segment is done. */
  private def drain(seg: Segment, out: Growable, onYield: () => Unit): Boolean =
    var st = Status.Emit
    while st == Status.Emit || st == Status.Yield do
      st = seg.step()
      if (out ne null) && (seg.out ne null) then out.append(seg.out, seg.outLen)
      if st == Status.Yield then onYield()
    if st != 0 && st != Status.Done then throw IllegalStateException(s"unexpected status ${Status.show(st)}")
    st == Status.Done

  /** Runs a stateless one-input segment over one chunk; returns all its outputs, in order.
   *  The segment is left ready for the next chunk (stateless: no state carries over). */
  def processStateless(seg: Segment, chunk: ArrayChunk, onYield: () => Unit = NoYield): ArrayChunk =
    val out = Growable(seg.out)
    seg.offer(0, chunk.array, chunk.offset, chunk.length)
    drain(seg, out, onYield)
    seg.release(0)
    out.result()

  /** Like [[processStateless]], but a failure is returned instead of thrown, together with the
   *  outputs produced before it: a parallel executor working ahead of demand can then deliver
   *  those, and raise the error only if the consumer gets that far. */
  def processUntilFailure(seg: Segment, chunk: ArrayChunk, onYield: () => Unit = NoYield): Processed =
    val out = Growable(seg.out)
    try
      seg.offer(0, chunk.array, chunk.offset, chunk.length)
      drain(seg, out, onYield)
      seg.release(0)
      Processed(out.result(), null)
    catch
      case e: InterruptedException => throw e
      case e: Throwable =>
        // Outputs of the failing step that were complete before the exception.
        if seg.out ne null then out.append(seg.out, seg.outLen)
        Processed(out.result(), e)

  /** Folds one chunk from scratch (the segment is reset first); returns the partial result. */
  def partialFold(seg: Segment, chunk: ArrayChunk, onYield: () => Unit = NoYield): Any =
    seg.reset()
    seg.offer(0, chunk.array, chunk.offset, chunk.length)
    seg.endOfInput(0)
    drain(seg, null, onYield)
    seg.result

  /** Feeds one chunk to a stateful segment (it keeps its state) and returns what it emitted.
   *  `done` tells whether the segment finished (e.g. a `take` completed). */
  def feed(seg: Segment, chunk: ArrayChunk, onYield: () => Unit = NoYield): (ArrayChunk, Boolean) =
    val out = Growable(seg.out)
    val done = seg.isDone || { seg.offer(0, chunk.array, chunk.offset, chunk.length); drain(seg, out, onYield) }
    (out.result(), done)

  /** Ends input 0 and runs the segment to completion: remaining outputs, and the fold result. */
  def end(seg: Segment, onYield: () => Unit = NoYield): (ArrayChunk, Any) =
    val out = Growable(seg.out)
    if !seg.isDone then
      seg.endOfInput(0)
      drain(seg, out, onYield)
    (out.result(), if seg.isFold then seg.result else null)

  /** An append-only primitive array of the same component type as `like`. */
  final class Growable(like: AnyRef):
    private val component: Class[?] = if like eq null then classOf[AnyRef] else like.getClass.getComponentType
    private var buf: AnyRef = java.lang.reflect.Array.newInstance(component, 0)
    private var size = 0
    def append(src: AnyRef, n: Int): Unit =
      if n > 0 then
        val cap = java.lang.reflect.Array.getLength(buf)
        if size + n > cap then
          val grown = java.lang.reflect.Array.newInstance(component, math.max(size + n, cap * 2))
          System.arraycopy(buf, 0, grown, 0, size)
          buf = grown
        System.arraycopy(src, 0, buf, size, n)
        size += n
    def result(): ArrayChunk = ArrayChunk(buf, 0, size)
