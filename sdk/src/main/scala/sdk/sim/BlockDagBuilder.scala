package sdk.sim

import cats.Parallel
import cats.effect.Sync
import cats.effect.kernel.Ref.Make
import cats.syntax.all.*
import sdk.sim.BlockDagSchedule.DagNode

/**
 * Language of a blockDAG builder.
 * Make an implementation of this, supply to [[BlockDagBuilder.build]] together with DAG schedule
 * to execute the whole blockDAG.
 */
trait BlockDagBuilder[DAG[_], M, S] {

  /**
   * Implementations should be thread safe
   * Add a node to the DAG. This can be either appending some string to build input for graphviz,
   * or actually executing the block by full node.
   * @param node it is enough to have only id, sender and justifications here.
   *             Everything else (transaction execution, adding offences) can be defined in the implementation.
   */
  def add(node: DagNode[M, S]): DAG[Unit]
}

object BlockDagBuilder {

  /** Build DAG according to a schedule and builder specified.*/
  def build[F[_]: Sync: Parallel: Make, M, S, T <: DagNode[M, S]](
    dagSchedule: BlockDagSchedule[F, M, S],
    builder: BlockDagBuilder[F, M, S],
  ): F[Unit] =
    dagSchedule.schedule
      .evalMap(nodes => nodes.parTraverse_(m => builder.add(m)))
      .compile
      .drain
}
