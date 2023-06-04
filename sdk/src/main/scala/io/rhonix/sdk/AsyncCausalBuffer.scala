package io.rhonix.sdk

import cats.effect.std.Queue
import cats.effect.{Async, Ref}
import cats.syntax.all._
import fs2.concurrent.SignallingRef
import fs2.{Pipe, Stream}

/**
 * Buffer aligning input against the causal order.
 * An item is emitted when and only when there are no dependencies or
 * upstream signalled that all dependencies are satisfied.
 */
trait AsyncCausalBuffer[F[_], T] {

  /**
   * Pipe that implements the buffer.
   * @param dependencies all dependencies of an item submitted.
   * @param dependenciesSatisfied filter dependencies that are satisfied.
   * @param concurrency concurrency makes sense only when dependenciesSatisfied takes long time.
   * @return
   */
  def pipe(
    dependencies: T => Set[T],
    dependenciesSatisfied: Set[T] => F[Set[T]],
    concurrency: Int = 1
  ): Pipe[F, T, T]

  /** Callback for an upstream to inform buffer that dependencies are processed. */
  def complete(x: Set[T]): F[Unit]
}

object AsyncCausalBuffer {

  /** Termination condition for a buffer stream is when input stream is finished and buffer is cleared. */
  final private case class Terminate(inputFinished: Boolean = false, bufferCleared: Boolean = false) {
    val done = inputFinished && bufferCleared
  }

  def apply[F[_]: Async, T](): F[AsyncCausalBuffer[F, T]] =
    Ref.of[F, DoublyLinkedDag[T]](DoublyLinkedDag.empty[T]).flatMap(apply[F, T])

  def apply[F[_]: Async, T](stRef: Ref[F, DoublyLinkedDag[T]]): F[AsyncCausalBuffer[F, T]] =
    (Queue.unbounded[F, T], SignallingRef.of(Terminate())).tupled.map { case (outQ, terminateRef) =>
      new AsyncCausalBuffer[F, T] {
        override def pipe(
          dependencies: T => Set[T],
          dependenciesSatisfied: Set[T] => F[Set[T]],
          concurrency: Int = 1
        ): Pipe[F, T, T] = (in: Stream[F, T]) => {
          val pull = (in.map(_.some) ++ Stream(none[T]))
            // record when input stream is finished
            .evalTap(x => terminateRef.update(_.copy(inputFinished = true)).whenA(x.isEmpty))
            .unNone
            .parEvalMapUnordered(concurrency) { x =>
              for {
                _ <- terminateRef.update(_.copy(bufferCleared = false))
                _ <- stRef.update(_.add(x, dependencies(x)))
                _ <- outQ.offer(x).whenA(dependencies(x).isEmpty)
                s <- dependenciesSatisfied(dependencies(x))
                _ <- complete(s)
              } yield ()
            }
          val out = Stream.fromQueueUnterminated(outQ).interruptWhen(terminateRef.map(_.done))

          out.concurrently(pull)
        }

        override def complete(x: Set[T]): F[Unit] = x.toList.traverse_ { y =>
          for {
            unlocked <- stRef.modify(_.remove(y))
            _ <- unlocked.toList.traverse(outQ.offer)
            buf <- stRef.get
            _ <- terminateRef.update(_.copy(bufferCleared = true)).whenA(buf.isEmpty)
          } yield ()
        }
      }
    }
}
