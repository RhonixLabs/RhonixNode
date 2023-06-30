package weaver.data

final case class MeldM[T](
  txs: List[T],                // transactions of a message
  conflictSet: Set[T],         // conflict set observed by a message
  conflictMap: Map[T, Set[T]], // conflicts between txs and conflict set
  dependsMap: Map[T, Set[T]],  // dependencies between txs and conflict set
  finalized: Set[T],           // transactions accepted finally by a message
  rejected: Set[T],            // transactions rejected finally by a message
)
