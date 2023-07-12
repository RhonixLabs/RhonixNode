//package sdk.node
//
//import cats.data.Kleisli
//import cats.effect.Ref
//import cats.effect.kernel.{Async, Clock, Outcome, Sync}
//import cats.effect.std.Queue
//import cats.syntax.all.*
//import fs2.Stream
//
//import scala.concurrent.duration.FiniteDuration
//
//final case class Retriever[F[_], O, I](
//  out: Stream[F, O],
//  enqueue: I => F[Unit],
//  getState: F[Retriever.ST[I]],
//)
//
///**
// * Component making sure that once item is required it is requested and received.
// */
//object Retriever {
//  trait RetrieveStatus
//
//  case class Requested(timestamp: FiniteDuration) extends RetrieveStatus
//
//  case object ReceiveStarted extends RetrieveStatus
//
//  def default[T]: ST[T] = ST(Map.empty[T, RetrieveStatus])
//
//  final case class ST[T](wantedSet: Set[T], requestedSet: Map[T, FiniteDuration], receivingSet: Set[T]) {
//
//    /**
//     * Item is required, make a request.
//     *
//     * @param now timestamp of request
//     */
//    def request(x: T): (ST[T], Boolean) = if (statuses.contains(x)) this -> false
//    else {
//      val newStatuses = copy(statuses = statuses + (x -> Requested(now)))
//      newStatuses -> true
//    }
//
//    def next(now: FiniteDuration) = copy(wantedSet = Set()) -> wantedSet
//
//    /**
//     * Receive started.
//     *
//     * @return if successful, timestamp of a request that item is received against.
//     */
//    def receiveStarted(x: T): (ST[T], Option[FiniteDuration]) = statuses
//      .get(x)
//      .collect { case Requested(timestamp) =>
//        val newStatuses = statuses + (x -> ReceiveStarted)
//        copy(statuses = newStatuses) -> timestamp.some
//      }
//      .getOrElse(this -> none[FiniteDuration])
//
//    /**
//     * Item is received in full.
//     */
//    def receiveDone(x: T): (ST[T], Boolean) = statuses
//      .get(x)
//      .collect { case ReceiveStarted =>
//        val newStatuses = statuses - x
//        copy(statuses = newStatuses) -> true
//      }
//      .getOrElse(this -> false)
//  }
//
//  /**
//   * Wrapper for receive function ensuring Retriever state integrity.
//   */
//  def receiveCase[F[_]: Sync, T, R](
//    stRef: Ref[F, ST[T]],
//    receive: T => F[R],
//  ): Kleisli[F, T, R] = Kleisli { (x: T) =>
//    def reRequest(timestamp: FiniteDuration) = for {
//      _ <- stRef.modify(_.receiveDone(x))
//      _ <- stRef.modify(_.request(x))
//    } yield ()
//
//    Sync[F].bracketCase(stRef.modify(_.receiveStarted(x)))(_ => receive(x)) {
//      // if receive has not been started - do nothing
//      case None -> _                             => Sync[F].unit
//      // if started and completed - notify the state
//      case _ -> Outcome.Succeeded(_)             => stRef.modify(_.receiveDone(x)).void
//      // if canceled - request again
//      case Some(timestamp) -> Outcome.Canceled() => reRequest(timestamp)
//      // if erred - request again
//      case Some(timestamp) -> Outcome.Errored(_) => reRequest(timestamp)
//    }
//  }
//
//  def apply[F[_]: Async, I, O](
//    stRef: Ref[F, ST[I]],
//    // function to request item
//    request: I => F[Unit],
//    // function to receive an item. This can be a long running operation like pulling a stream.
//    receive: I => F[O],
//  ): F[Retriever[F, I, O]] = Queue.synchronous[F, Unit].flatMap { reqQ =>
//    def req(x: I) = stRef.modify(_.request(x)) >> reqQ.offer(())
//
//    def rcv(x: I) = receiveCase(stRef, receive).run(x)
//
//    val requestStream = Stream
//      .fromQueueUnterminated(reqQ)
//      .evalMap { _ =>
//        for {
//          now <- Clock[F].realTime
//          x   <- stRef.modify(_.next(now))
//          _   <- x.toList.traverse(request)
//        } yield ()
//      }
//
//    val receiveStream = Stream.fromQueueUnterminated(rcvQ).evalMap(rcv)
//
//    Retriever(receiveStream concurrently requestStream, reqQ.offer, stRef.get)
//  }
//}
