package strom.runtime

import scala.compiletime.uninitialized

/** A compiled, resumable, chunk-driven pipeline segment.
 *
 *  Instances are produced by the staged code generator (anonymous subclasses overriding
 *  [[stepImpl]]). A segment is synchronous, effect-free and thread-confined: it must only be
 *  driven from one thread at a time, and it never blocks or performs I/O.
 *
 *  Protocol (see [[Segment.Status]]):
 *   - `offer(port, array, offset, length)` lends an input chunk to the segment. The array must
 *     stay unmodified until the segment asks for more input on that port.
 *   - `endOfInput(port)` marks the port as finished.
 *   - `step()` runs for a bounded amount of work (at most `budget` producer steps, and at most
 *     `maxOut` outputs) and returns a status code. Whatever the status, `out(0 until outLen)`
 *     holds outputs produced by this step; they stay valid until the next `step()` call.
 *
 *  If `stepImpl` throws, the instance is poisoned: every later `step()` rethrows.
 */
abstract class Segment(val numInputs: Int, val maxOut: Int, val budget: Int) extends Slots:
  require(numInputs >= 0 && numInputs <= Segment.MaxPorts, s"numInputs must be in 0..${Segment.MaxPorts}")
  require(maxOut > 0, "maxOut must be positive")
  require(budget > 0, "budget must be positive")

  // ---- input ports (fields so that generated code can read them without indirection) ----
  var in0: AnyRef = uninitialized; var pos0: Int = 0; var end0: Int = 0; var ended0: Boolean = false
  var in1: AnyRef = uninitialized; var pos1: Int = 0; var end1: Int = 0; var ended1: Boolean = false
  var in2: AnyRef = uninitialized; var pos2: Int = 0; var end2: Int = 0; var ended2: Boolean = false
  var in3: AnyRef = uninitialized; var pos3: Int = 0; var end3: Int = 0; var ended3: Boolean = false

  // ---- output ----
  /** Output buffer, a primitive array of length `maxOut` allocated once by the generated subclass. */
  var out: AnyRef = uninitialized
  var outLen: Int = 0

  // ---- control ----
  var fuel: Int = 0
  var started: Boolean = false
  var finished: Boolean = false
  /** Final value of a fold segment (boxed once, when the segment finishes). */
  var result: Any = uninitialized
  private var poison: Throwable = null

  /** Generated body. Returns a [[Segment.Status]] code. */
  protected def stepImpl(): Int

  /** Whether this segment folds its input into [[result]] (as opposed to emitting into [[out]]). */
  def isFold: Boolean

  final def step(): Int =
    if poison ne null then throw IllegalStateException("segment is poisoned by an earlier failure", poison)
    try stepImpl()
    catch
      case t: Throwable =>
        poison = t
        throw t

  final def isPoisoned: Boolean = poison ne null
  final def isDone: Boolean = finished

  final def offer(port: Int, array: AnyRef, offset: Int, length: Int): Unit =
    if port < 0 || port >= numInputs then throw IndexOutOfBoundsException(s"port $port")
    if remaining(port) != 0 then throw IllegalStateException(s"port $port still has unconsumed input")
    if isEnded(port) then throw IllegalStateException(s"port $port already ended")
    (port: @annotation.switch) match
      case 0 => in0 = array; pos0 = offset; end0 = offset + length
      case 1 => in1 = array; pos1 = offset; end1 = offset + length
      case 2 => in2 = array; pos2 = offset; end2 = offset + length
      case 3 => in3 = array; pos3 = offset; end3 = offset + length

  final def endOfInput(port: Int): Unit = (port: @annotation.switch) match
    case 0 => ended0 = true
    case 1 => ended1 = true
    case 2 => ended2 = true
    case 3 => ended3 = true
    case _ => throw IndexOutOfBoundsException(s"port $port")

  final def remaining(port: Int): Int = (port: @annotation.switch) match
    case 0 => end0 - pos0
    case 1 => end1 - pos1
    case 2 => end2 - pos2
    case 3 => end3 - pos3
    case _ => throw IndexOutOfBoundsException(s"port $port")

  final def isEnded(port: Int): Boolean = (port: @annotation.switch) match
    case 0 => ended0
    case 1 => ended1
    case 2 => ended2
    case 3 => ended3
    case _ => throw IndexOutOfBoundsException(s"port $port")

  /** Makes the instance reusable for a new, independent run (e.g. one chunk of a parallel
   *  partial fold). All staged state is re-initialised by the next `step()`. */
  final def reset(): Unit =
    if poison ne null then throw IllegalStateException("cannot reset a poisoned segment", poison)
    started = false; finished = false; result = null; outLen = 0
    var p = 0
    while p < numInputs do
      release(p)
      p += 1
    ended0 = false; ended1 = false; ended2 = false; ended3 = false

  /** Drops the reference to a consumed input array so it can be reclaimed / reused. */
  final def release(port: Int): Unit = (port: @annotation.switch) match
    case 0 => in0 = null; pos0 = 0; end0 = 0
    case 1 => in1 = null; pos1 = 0; end1 = 0
    case 2 => in2 = null; pos2 = 0; end2 = 0
    case 3 => in3 = null; pos3 = 0; end3 = 0
    case _ => throw IndexOutOfBoundsException(s"port $port")

object Segment:
  final val MaxPorts = 4

  /** Status codes returned by [[Segment.step]]. Non-negative codes mean "need input on port n". */
  object Status:
    final val Emit = -1    // output buffer is full
    final val Yield = -2   // work budget exhausted; call step() again
    final val Done = -3    // stream finished; `out` holds the last outputs / `result` is set
    /** Internal: returned by outlined parts of a split step method to mean "keep going". */
    final val Continue = Int.MinValue

    def show(s: Int): String = s match
      case Emit     => "Emit"
      case Yield    => "Yield"
      case Done     => "Done"
      case Continue => "Continue"
      case n        => s"NeedInput($n)"

/** Creates fresh segment instances for one compiled pipeline. */
final class SegmentFactory[Out](
    val numInputs: Int,
    val isFold: Boolean,
    val plan: PlanInfo,
    mk: (Int, Int) => Segment,
):
  def create(maxOut: Int = 1024, budget: Int = 4096): Segment = mk(maxOut, budget)
  override def toString = s"SegmentFactory(inputs=$numInputs, fold=$isFold, $plan)"

/** Base for one-shot direct pipelines that were too large for a single JIT-able method and
 *  therefore run with field storage and split methods. */
abstract class DirectFrame[R] extends Slots:
  def runImpl(): R
