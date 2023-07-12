package sdk.node

import cats.effect.kernel.{Async, Outcome}
import cats.effect.std.Queue
import cats.effect.{Ref, Sync}
import cats.syntax.all.*
import fs2.Stream

object Proposer {

  sealed trait ProposerStatus
  // ready to create a block
  case object Idle     extends ProposerStatus
  // proposal is being created
  case object Creating extends ProposerStatus
  // proposal is created, awaiting validation and adding to local block dag state
  case object Adding   extends ProposerStatus

  /** Default state is Idle. */
  def default: ST = ST(Idle, scheduled = false)

  final case class ST(status: ProposerStatus, scheduled: Boolean) {

    def schedule = copy(scheduled = true)

    /**
     * Start proposing.
     */
    def start: (ST, Boolean) = status match {
      // proposal should be possible only if the previous one is done
      case Idle if scheduled  => ST(Creating, scheduled = false) -> true
      case Idle if !scheduled => this                            -> false
      case _                  => this                            -> false
    }

    /**
     * Notify that proposal is created.
     */
    def created: ST = status match {
      case Creating => copy(status = Adding)
      case _        => this
    }

    /**
     * Notify that proposal is complete and the next one can be triggered.
     */
    def done: (ST, Boolean) = status match {
      case Adding => copy(status = Idle) -> true
      case _      => this                -> false
    }
  }

  /**
   * Wrapper for propose function ensuring Proposer state integrity.
   */
  def proposeBracket[F[_]: Sync, P](stRef: Ref[F, ST], propose: F[P]): F[Either[Unit, P]] =
    Sync[F].bracketCase(stRef.modify(_.start)) { go =>
      go.guard[Option].toRight(()).traverse(_ => propose)
    } {
      // if started and completed - notify the state
      case true -> Outcome.Succeeded(_) => stRef.update(_.created)
      // if started but canceled - return to initial idle state
      case true -> Outcome.Canceled()   => stRef.update(_.created.done._1)
      // if erred - return to initial idle state, rise error
      case (_, Outcome.Errored(e))      => stRef.update(_.created.done._1) >> e.raiseError[F, Unit]
      // if not started - do nothing
      case false -> _                   => Sync[F].unit
    }

  /**
   * @param stRef   proposer state n a
   * @param propose function to create proposal
   *                
   * @return infinite stream of proposals and callback to trigger proposal.
   */
  def apply[F[_]: Async, O](stRef: Ref[F, ST], propose: F[O]): F[(Stream[F, O], F[Unit])] =
    Queue.synchronous[F, Unit].map { inQ =>
      def schedule = stRef.update(_.schedule) >> inQ.offer(())

      val proposeStream = Stream
        .fromQueueUnterminated(inQ)
        // concurrency here is 2 to be able to make proposals and concurrently pull failed attempts
        .parEvalMapUnordered(2)(_ => proposeBracket[F, O](stRef, propose))
        .collect { case Right(x) => x }
        // after successful propose try propose again
        .evalTap(_ => inQ.tryOffer(()))

      (proposeStream, schedule)
    }
}
