package dproc

import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.Stream

object NodeFlow {

  // Effects that DProc has to execute
  trait Effects[F[_], M, S, T, R, B] {
    def bufferIncoming(m: M): F[Unit]
    def added(m: M, result: R): F[Unit]
    def store(b: B): F[Unit]
    def triggerProcess(m: M): F[Unit]
    def triggerPropose: F[Unit]
  }

  // The logic of composing input streams and invoking effects
  def apply[F[_]: Async, M, S, T, R, B](
    // streams
    input: Stream[F, M],
    processed: Stream[F, (M, R)],
    transactions: Stream[F, T],
    proposals: Stream[F, B],
    // effects
    effects: Effects[F, M, S, T, R, B],
    id: B => M,
  ): Stream[F, Unit] = {
    import effects.*

    val pullIncoming          = input.evalMap(bufferIncoming)
    val pullResultsAndReport  = processed.evalTap { case (m, r) => added(m, r) }
    val pullProposals         = proposals.evalTap(b => store(b) >> triggerProcess(id(b)))
    val pullAllThroughPropose = (pullResultsAndReport merge transactions).evalTap(_ => triggerPropose)

    // Stream is closed when pullIncoming stream is closed
    pullIncoming concurrently pullAllThroughPropose concurrently pullProposals
  }
}
