package dproc.data

import weaver.Gard.GardM
import weaver.data._

/**
 * All data required to be packed in a block.
 * This does not include data required from execution engine to do execution.
 * @param sender block sender
 * @param minGenJs minimal generative justifications set
 * @param offences offences computed by the message
 * @param txs transactions executed in a block
 * @param finalFringe final fringe computed by the message
 * @param finalized finalization computed by the message
 * @param merge rejections on conflict set merge computed by the message
 * @param bonds bonds map of a message
 * @param lazTol laziness tolerance
 * @param expThresh transaction expiration threshold
 * @tparam M
 * @tparam S
 * @tparam T
 */
final case class Block[M, S, T](
  sender: S,
  minGenJs: Set[M],
  offences: Set[M],
  txs: List[T],
  finalFringe: Set[M],
  finalized: Option[ConflictResolution[T]],
  merge: Set[T],
  bonds: Bonds[S],
  lazTol: Int,
  expThresh: Int
)

object Block {

  trait Identifier[M, S] {
    def id: M // ID (hash)
    def sender: S // block sender
    def minGenJs: Set[M] // minimal generative justifications set)
    def offences: Set[M] // offences computed by the message
  }

  final case class WithId[M, S, T](id: M, m: Block[M, S, T])

  def toLazoM[M, S, T](m: Block[M, S, T]) = LazoM(
    m.sender,
    m.minGenJs,
    m.offences,
    LazoF(m.finalFringe),
    LazoE(m.bonds, m.lazTol, m.expThresh)
  )

  def toLazoE[M, S, T](m: Block[M, S, T]) =
    LazoE(m.bonds, m.lazTol, m.expThresh)

  def toGardM[M, S, T](m: Block[M, S, T]): GardM[M, T] = GardM(m.txs.toSet, m.finalFringe)
}
