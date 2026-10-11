package strom.runtime

/** How a pipeline may be spread over several cores. Produced at compile time (by
 *  `StromFs2.fusedPar`, `StromZio.fusedPar` or `LoomParallel.plan`) from the static metadata of
 *  the pipeline; executed by an adapter.
 *
 *  Tiers run one after another, connected by bounded queues of chunks. Within a tier:
 *   - [[Tier.Replicated]]: the segment is stateless, so each input chunk can be processed by
 *     any of N instances independently; outputs are put back in input order (reorder buffer).
 *   - [[Tier.PartialFold]]: an associative fold; each chunk is folded separately and the
 *     partial results are combined in input order.
 *   - [[Tier.Single]]: a stateful segment: one instance. The pipeline is cut here, so the
 *     stateless parts around it still run in parallel.
 *
 *  Every plan computes exactly what the sequential pipeline computes, in the same order. */
final class ParallelPlan[Out](val tiers: Vector[Tier], val inputs: Int):
  require(tiers.nonEmpty)
  def isFold: Boolean = tiers.last match
    case _: Tier.PartialFold => true
    case Tier.Single(f)      => f.isFold
    case _: Tier.Replicated  => false
  override def toString = tiers.mkString(" | ")

enum Tier:
  case Replicated(factory: SegmentFactory[?])
  case PartialFold(factory: SegmentFactory[?], combine: (Any, Any) => Any)
  case Single(factory: SegmentFactory[?])

  override def toString = this match
    case Replicated(f)     => s"replicated[${f.plan}]"
    case PartialFold(f, _) => s"partialFold[${f.plan}]"
    case Single(f)         => s"single[${f.plan}]"
