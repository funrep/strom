package strom.runtime

/** How a pipeline may be spread over several cores. Produced at compile time by
 *  `Strom.parallel*` from the static metadata of the pipeline; executed by an adapter.
 *
 *  Tiers run one after another, connected by bounded queues of chunks. Within a tier:
 *   - [[Tier.Replicated]]: the segment is stateless, so each input chunk can be processed by
 *     any of N instances independently; outputs are put back in input order (reorder buffer).
 *   - [[Tier.PartialFold]]: an associative fold; each chunk is folded separately and the
 *     partial results are combined in input order.
 *   - [[Tier.Keyed]]: a stateful segment, one instance per partition; elements are routed by
 *     key so that all elements of a key are seen by one instance, in order. Outputs of
 *     different partitions interleave.
 *   - [[Tier.Single]]: a stateful, non-partitionable segment: one instance. The pipeline is cut
 *     here, so the cheap stateless prefix still runs in parallel before it.
 */
final class ParallelPlan[Out](val tiers: Vector[Tier], val inputs: Int):
  require(tiers.nonEmpty)
  def isFold: Boolean = tiers.last match
    case _: Tier.PartialFold => true
    case Tier.Single(f)      => f.isFold
    case Tier.Keyed(f, _)    => f.isFold
    case _: Tier.Replicated  => false
  override def toString = tiers.mkString(" | ")

enum Tier:
  case Replicated(factory: SegmentFactory[?])
  case PartialFold(factory: SegmentFactory[?], combine: (Any, Any) => Any)
  case Keyed(factory: SegmentFactory[?], partitioner: Partitioner)
  case Single(factory: SegmentFactory[?])

  override def toString = this match
    case Replicated(f)     => s"replicated[${f.plan}]"
    case PartialFold(f, _) => s"partialFold[${f.plan}]"
    case Keyed(f, _)       => s"keyed[${f.plan}]"
    case Single(f)         => s"single[${f.plan}]"

/** Routes the elements of a chunk to partitions by key. Generated (specialised for the element
 *  type, with the key function inlined); allocates per chunk, never per element. */
abstract class Partitioner:
  /** Splits `array(offset until offset + length)` into `parts` primitive arrays, preserving order. */
  def route(array: AnyRef, offset: Int, length: Int, parts: Int): Array[AnyRef]
