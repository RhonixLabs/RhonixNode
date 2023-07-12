package sdk.sim

import cats.syntax.all.*

import scala.collection.immutable.Queue

/**
 * Class modelling p2p connections.
 * For each sender - queue of messages from peers.
 */
final case class P2P[M, S](private val x: Map[S, Map[S, Queue[M]]]) {

  def send(s: S, m: M): P2P[M, S] = {
    // for inner map of x under key s enqueue m to all queues that are values of a that inner map
    val r = x.keys.foldLeft(x) { case (acc, k) => acc.updatedWith(k)(_.map(_.updatedWith(s)(_.map(_.enqueue(m))))) }
    P2P(r)
  }

  def observe(s: S, toObserve: Map[S, Int]): (P2P[M, S], Map[S, Option[M]]) = {
    // return copy of Buffer with queues for each sender specified in toObserve dequeued for
    // number of element specified in toObserve. Queues are stored in x for sender s
    val (observed, rest) = x
      .get(s)
      .map(_.partition { case (sId, _) => toObserve.contains(sId) })
      .getOrElse(Map.empty[S, Queue[M]] -> Map.empty[S, Queue[M]])
    val v                = observed.map { case x @ (sId, queue) =>
      val (observed, rest) = queue.splitAt(toObserve(sId))
      sId -> (observed.lastOption -> rest)
    }
    val latestUpdate     = v.view.mapValues(_._1) ++ rest.view.mapValues(_ => none[M])
    val newObserved      = v.view.mapValues(_._2)
    P2P(x.updated(s, rest ++ newObserved)) -> latestUpdate.toMap
  }
}

object P2P {
  def empty[M, S](senders: Set[S]): P2P[M, S] = P2P(senders.map(_ -> senders.map(_ -> Queue.empty[M]).toMap).toMap)
}
