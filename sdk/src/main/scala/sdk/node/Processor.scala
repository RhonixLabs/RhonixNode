package sdk.node

import cats.data.Kleisli
import cats.effect.kernel.Async
import cats.effect.std.Queue
import cats.effect.{Outcome, Ref, Sync}
import cats.syntax.all.*
import fs2.Stream

object Processor {
  def default[T](concurrency: Int = 16): ST[T] =
    ST(Set.empty[T], Vector.empty[T], concurrency)

  final case class ST[T](
    processingSet: Set[T],
    waitingList: Vector[T],
    concurrency: Int,
  ) {

    /**
     * Attempt to add the item.
     * If prepend is true item os added to the beginning of a waiting in a position to be processed next.
     *
     * @return new state and true if the item is added or false if it is ignored.
     */
    private def attemptAdd(x: T, prepend: Boolean): (ST[T], Boolean) =
      if (processingSet.contains(x) || waitingList.contains(x)) this -> false
      else {
        val newWL = if (prepend) x +: waitingList else waitingList :+ x
        copy(waitingList = newWL) -> true
      }

    /**
     * Attempt to append the item to waiting list.
     */
    def receive(x: T): (ST[T], Boolean) = attemptAdd(x, prepend = false)

    /**
     * Attempt to prepend the item to waiting list.
     */
    def retry(x: T): (ST[T], Boolean) = attemptAdd(x, prepend = true)

    /**
     * Record processing completion.
     */
    def done(x: T): ST[T] = copy(processingSet = processingSet - x)

    /**
     * Get next to process.
     * If nothing in the waiting list of concurrency limit is hit - None is returned.
     */
    def next: (ST[T], Option[T]) =
      if (processingSet.sizeIs >= concurrency)
        this -> none[T]
      else
        waitingList match {
          case h +: tail => copy(waitingList = tail, processingSet = processingSet + h) -> h.some
          case _         => this                                                        -> none[T]
        }
  }

  /**
   * Wrapper for process function ensuring Processor state integrity.
   */
  def processCase[F[_]: Sync, B, R](
    stRef: Ref[F, ST[B]],
    process: B => F[R],
  ): Kleisli[F, B, R] = Kleisli { (b: B) =>
    val retry = stRef.modify(_.done(b).retry(b)).void
    Sync[F].guaranteeCase(process(b)) {
      // if completed - notify the state
      case Outcome.Succeeded(_) => stRef.update(_.done(b))
      // if canceled - schedule processing retry
      case Outcome.Canceled()   => retry
      // if erred - schedule processing retry
      case Outcome.Errored(_)   => retry
    }
  }

  def apply[F[_]: Async, I, O](
    stRef: Ref[F, ST[I]],
    process: I => F[O],
  ): F[(Stream[F, (I, O)], I => F[Unit])] = Queue.synchronous[F, Unit].map { inQ =>
    def receive(i: I): F[Unit] = stRef.modify(_.receive(i)).flatMap(inQ.offer(()).whenA)

    val out = Stream
      .fromQueueUnterminated(inQ)
      // try to get next to process
      .evalMap(_ => stRef.modify(_.next))
      .unNone
      // For input queue (inQ) to not semantically block on offering, parallelism here this should
      // be equal concurrency exposed through `next` method of a state + 1. No more then that amount of items
      // can be sent to processing concurrently, so if parallelism here is greater - there will always be a taker
      // for an offer of synchronous queue.
      // Since concurrency can be tuned by assigning value inside the state - parallelism here is unbounded.`
      .parEvalMapUnorderedUnbounded[F, (I, O)] { b =>
        processCase(stRef, process).run(b).map(b -> _)
      }

    out -> receive
  }
}
