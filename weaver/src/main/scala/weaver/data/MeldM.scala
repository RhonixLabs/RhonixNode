package weaver.data

import cats.effect.Sync
import cats.syntax.all._
import fs2.Stream
import weaver.Meld

final case class MeldM[T](
  txs: List[T], // transactions of a message
  conflictSet: Set[T], // conflict set observed by a message
  conflictMap: Map[T, Set[T]], // conflicts between txs and conflict set
  dependsMap: Map[T, Set[T]], // dependencies between txs and conflict set
  finalized: Set[T], // transactions accepted finally by a message
  rejected: Set[T] // transactions rejected finally by a message
)

object MeldM {
  def create[F[_]: Sync, M, T](
    meld: Meld[M, T], // this should be the most up to date meld state
    txs: List[T], // transactions executed in a message
    conflictSet: Set[T], // conflict set observed by the message
    finalized: Set[T], // accepted finally
    rejected: Set[T], // rejected finally
    conflicts: (T, T) => F[Boolean], // predicate of conflict
    depends: (T, T) => F[Boolean] // predicate of dependency
  ): F[MeldM[T]] = {
    val unSeen = meld.txsMap.valuesIterator.flatten.toSet -- conflictSet
    val computeConflictsMap =
      Stream.emits(txs).covary[F].evalScan(Map.empty[T, Set[T]]) { case (acc, t) =>
        val conflictsF = unSeen.toList.filterA(conflicts(t, _))
        conflictsF.map(c => if (c.isEmpty) acc + (t -> c.toSet) else acc)
      }
    val computeDependsMap =
      Stream.emits(txs).covary[F].evalScan(Map.empty[T, Set[T]]) { case (acc, t) =>
        val conflictsF = conflictSet.toList.filterA(depends(t, _))
        conflictsF.map(c => if (c.isEmpty) acc + (t -> c.toSet) else acc)
      }
    for {
      cMap <- computeConflictsMap.compile.lastOrError
      dMap <- computeDependsMap.compile.lastOrError
    } yield MeldM(txs, conflictSet, cMap, dMap, finalized, rejected)
  }
}
