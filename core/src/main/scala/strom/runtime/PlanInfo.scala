package strom.runtime

/** How many outputs an operator produces per input. */
enum Cardinality:
  case OneToOne, AtMostOne, Many, Bounded

/** Static metadata about one operator of a staged pipeline. */
final case class StageInfo(op: String, stateless: Boolean, cardinality: Cardinality, associative: Boolean = false):
  override def toString =
    val flags = List(if stateless then "stateless" else "stateful", cardinality.toString) ++
      (if associative then List("associative") else Nil)
    s"$op[${flags.mkString(",")}]"

/** Static metadata about a whole staged pipeline, used by the parallel planner.
 *  `stages` is a pre-order listing (sources first, zip operands left before right). */
final case class PlanInfo(inputs: Int, stages: List[StageInfo], fold: Boolean):
  /** All operators are stateless: the segment may be replicated over N workers (plus reordering). */
  def stateless: Boolean = stages.forall(s => s.stateless || s.op.startsWith("source"))
  /** The terminal is a fold whose operator is associative: partial folds may be combined. */
  def associativeFold: Boolean = fold && stages.lastOption.exists(_.associative)
  override def toString = stages.mkString(" → ")
