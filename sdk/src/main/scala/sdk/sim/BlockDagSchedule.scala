package sdk.sim

import cats.Parallel
import cats.effect.Sync
import cats.effect.kernel.Ref
import cats.effect.kernel.Ref.Make
import cats.syntax.all.*
import fs2.Stream
import BlockDagSchedule.DagNode
import NetworkSchedule.ToCreate

final case class BlockDagSchedule[F[_], M, S](senders: Set[S], schedule: Stream[F, List[DagNode[M, S]]])
object BlockDagSchedule {
  case class DagNode[M, S](id: M, sender: S, js: Map[S, M])

  def apply[F[_]: Sync: Parallel: Make, M, S](networkSchedule: NetworkSchedule[M, S]): BlockDagSchedule[F, M, S] = {
    import networkSchedule.*
    // what is yet to be observed
    val incomingRef = Ref.unsafe[F, P2P[M, S]](P2P.empty(networkSchedule.senders))
    // latest messages per sender
    val lmsRef      = Ref.unsafe[F, Map[S, Map[S, M]]](senders.map(_ -> Map.empty[S, M]).toMap)

    val x: fs2.Stream[F, List[DagNode[M, S]]] = fs2.Stream
      .fromIterator[F](schedule.iterator, 1)
      .evalMap { toCreate =>
        toCreate.toList
          // observe according to schedule, prepare DagNodes
          .traverse { case s -> ToCreate(id, toObserve) =>
            incomingRef.modify(_.observe(s, toObserve)).map(_.collect { case (s, Some(m)) => s -> m }).flatMap {
              observedJs =>
                lmsRef
                  .modify { curV =>
                    val newJs = curV.getOrElse(s, Map.empty[S, M]) ++ observedJs
                    (curV + (s -> newJs), newJs)
                  }
                  .map(DagNode[M, S](id, s, _))
            }
          }
          // push created nodes to the inboxes
          .flatTap(_.traverse_(m => incomingRef.update(_.send(m.sender, m.id))))
      }
    BlockDagSchedule[F, M, S](networkSchedule.senders, x)
  }
}
