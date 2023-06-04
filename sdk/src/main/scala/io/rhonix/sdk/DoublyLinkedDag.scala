package io.rhonix.sdk

final case class DoublyLinkedDag[M](childMap: Map[M, Set[M]], parentsMap: Map[M, Set[M]]) {
  def add(m: M, parents: Set[M]): DoublyLinkedDag[M] = {
    val newChildMap = parents.foldLeft(childMap) { case (acc, d) =>
      acc + (d -> acc.get(d).map(_ + m).getOrElse(Set(m)))
    }
    val newMissingMap = if (parents.isEmpty) parentsMap else parentsMap + (m -> parents)
    DoublyLinkedDag(newChildMap, newMissingMap)
  }

  def remove(m: M): (DoublyLinkedDag[M], Set[M]) = {
    val awaiting = childMap.getOrElse(m, Set())
    val (adjusted, unaltered) = parentsMap.view.partition { case (m, _) => awaiting.contains(m) }
    val (done, altered) = adjusted.mapValues(_ - m).partition { case (_, deps) => deps.isEmpty }
    val doneSet = done.keys.toSet

    val newChildMap = childMap - m
    val newMissingMap = (unaltered ++ altered).filterNot { case (k, _) =>
      (doneSet + m).contains(k)
    }.toMap
    DoublyLinkedDag(newChildMap, newMissingMap) -> doneSet
  }

  def contains(m: M): Boolean = childMap.contains(m)
  def isEmpty: Boolean = childMap.isEmpty && parentsMap.isEmpty
}

object DoublyLinkedDag {
  def empty[M]: DoublyLinkedDag[M] = DoublyLinkedDag[M](Map(), Map())
}
