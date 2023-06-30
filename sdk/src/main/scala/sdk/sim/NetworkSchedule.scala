package sdk.sim

import NetworkSchedule.ToCreate

/**
 * Schedule for DAG creation. Each item in LazyList contains a set of messages to create.
 *
 * As messages without causal relation can be processed concurrently, each item in LazyList is a Set.
 * One edge case is a block chain, where Set is of a single item and all senders produce blocks one by one.
 * Another edge case is a full throttle DAG, when all senders always create blocks concurrently.
 * The latter should be used to estimate the maximum throughput of the system.
 */
final case class NetworkSchedule[M, S](senders: Set[S], schedule: LazyList[Map[S, ToCreate[M, S]]])
object NetworkSchedule {

  /** Observe zero or several messages from peers and make new block by creator with ID assigned. */
  final case class ToCreate[M, S](assignId: M, toObserve: Map[S, Int])

  /** Network partitioning. Sender pair do not see each other through the range. */
  final case class Isolation[S](range: Range.Inclusive, link: (S, S))

  /** Max throughput schedule with all validators creating blocks concurrently and synchronously. */
  def fullThrottleSchedule[M, S](senders: Set[S], assignId: S => M): NetworkSchedule[M, S] = {
    val schedule = LazyList.continually(senders.map(s => s -> ToCreate(assignId(s), senders.map(_ -> 1).toMap)).toMap)
    NetworkSchedule(senders, schedule)
  }

  /** Add partitioning to the schedule. */
  def withIsolation[M, S](schedule: NetworkSchedule[M, S], isolation: Isolation[S]): NetworkSchedule[M, S] = {
    val newSchedule = schedule.schedule.zipWithIndex.map { case (lvl, i) =>
      if (isolation.range.contains(i))
        lvl.map { case x @ (s, toCreate) =>
          if (isolation.link._1 == s)
            s -> ToCreate(toCreate.assignId, toCreate.toObserve + (isolation.link._2 -> 0))
          else if (isolation.link._2 == s)
            s -> ToCreate(toCreate.assignId, toCreate.toObserve + (isolation.link._1 -> 0))
          else x
        }
      else lvl
    }

    schedule.copy(schedule = newSchedule)
  }
}
