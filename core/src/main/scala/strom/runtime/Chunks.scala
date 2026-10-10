package strom.runtime

import strom.runtime.Segment.Status

/** A slice of a primitive array: the unit of work between tiers. */
final case class ArrayChunk(array: AnyRef, offset: Int, length: Int):
  def elements: Iterator[Any] = Iterator.range(offset, offset + length).map(java.lang.reflect.Array.get(array, _))

/** Synchronous, chunk-at-a-time operations on segments, shared by all parallel executors.
 *  They allocate per chunk (the result array), never per element. `onYield` is called when a
 *  step uses up its budget, so a driver can give way (e.g. `Thread.yield()`). */
object Chunks:
  private val NoYield: () => Unit = () => ()

  /** Runs a stateless one-input segment over one chunk; returns all its outputs, in order.
   *  The segment is left ready for the next chunk (stateless: no state carries over). */
  def processStateless(seg: Segment, chunk: ArrayChunk, onYield: () => Unit = NoYield): ArrayChunk =
    val out = Growable(seg.out)
    seg.offer(0, chunk.array, chunk.offset, chunk.length)
    var continue = true
    while continue do
      val st = seg.step()
      out.append(seg.out, seg.outLen)
      st match
        case 0            => continue = false
        case Status.Yield => onYield()
        case Status.Emit  => ()
        case Status.Done  => continue = false
        case other        => throw IllegalStateException(s"unexpected status ${Status.show(other)}")
    seg.release(0)
    out.result()

  /** Folds one chunk from scratch (the segment is reset first); returns the partial result. */
  def partialFold(seg: Segment, chunk: ArrayChunk, onYield: () => Unit = NoYield): Any =
    seg.reset()
    seg.offer(0, chunk.array, chunk.offset, chunk.length)
    seg.endOfInput(0)
    finish(seg, onYield)

  /** Feeds one chunk to a stateful segment (it keeps its state) and returns what it emitted.
   *  `done` tells whether the segment finished (e.g. a `take` completed). */
  def feed(seg: Segment, chunk: ArrayChunk, onYield: () => Unit = NoYield): (ArrayChunk, Boolean) =
    val out = Growable(seg.out)
    if seg.isDone then (out.result(), true)
    else
      seg.offer(0, chunk.array, chunk.offset, chunk.length)
      var done = false
      var continue = true
      while continue do
        val st = seg.step()
        if seg.out ne null then out.append(seg.out, seg.outLen)
        st match
          case 0            => continue = false
          case Status.Yield => onYield()
          case Status.Emit  => ()
          case Status.Done  => continue = false; done = true
          case other        => throw IllegalStateException(s"unexpected status ${Status.show(other)}")
      (out.result(), done)

  /** Ends input 0 and runs the segment to completion: remaining outputs, and the fold result. */
  def end(seg: Segment, onYield: () => Unit = NoYield): (ArrayChunk, Any) =
    val out = Growable(seg.out)
    if !seg.isDone then
      seg.endOfInput(0)
      var st = seg.step()
      if seg.out ne null then out.append(seg.out, seg.outLen)
      while st != Status.Done do
        if st == Status.Yield then onYield()
        st = seg.step()
        if seg.out ne null then out.append(seg.out, seg.outLen)
    (out.result(), if seg.isFold then seg.result else null)

  private def finish(seg: Segment, onYield: () => Unit): Any =
    var st = seg.step()
    while st != Status.Done do
      if st == Status.Yield then onYield()
      st = seg.step()
    seg.result

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
