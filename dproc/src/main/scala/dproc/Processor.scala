package dproc

import cats.effect.kernel.Async
import cats.effect.{Ref, Sync}
import cats.syntax.all._
import dproc.Errors.IncompleteMessageInProcessQueue
import dproc.MessageLogic.validateMessage
import dproc.data.Block
import fs2.Stream
import fs2.concurrent.Channel
import weaver.Gard.GardM
import weaver.Weaver.{AddResult, ExeEngine}
import weaver.data.{LazoM, MeldM}
import weaver.rules.Dag
import weaver.syntax.all._
import weaver.{Lazo, Offence, Weaver}

final case class Processor[F[_], M, S, T](
  stRef: Ref[F, Processor.ST[M]],
  accept: Block.WithId[M, S, T] => F[Unit],
  results: Stream[F, (M, AddResult[M, S, T])]
)

/** Message processor - add messages to the state. */
object Processor {

  final case class ST[M](processing: Set[M]) {
    def begin(m: M): (ST[M], Boolean) =
      if (processing.contains(m)) this -> false
      else copy(processing = processing + m) -> true

    def done(m: M): (ST[M], Unit) = copy(processing = processing - m) -> ()
  }

  final private case class ReplayResult[M, S, T](
    lazoME: LazoM.Extended[M, S],
    meldMOpt: Option[MeldM[T]],
    gardMOpt: Option[GardM[M, T]],
    offenceOpt: Option[Offence]
  )

  /**
   * Repeat the process of executing a block.
   * @param m block
   * @param s state to execute block against
   * @param exeEngine execution engine
   *
   */
  private def replay[F[_]: Sync, M, S, T: Ordering](
    m: Block.WithId[M, S, T],
    s: Weaver[M, S, T],
    exeEngine: ExeEngine[F, M, S, T]
  ): F[ReplayResult[M, S, T]] = Sync[F].defer {
    val complete = Lazo.canAdd(m.m.minGenJs, m.m.sender, s.lazo)
    lazy val conflictSet = Dag.between(m.m.minGenJs, m.m.finalFringe, s.lazo.seenMap).flatMap(s.meld.txsMap)
    lazy val mkMeld = MeldM.create[F, M, T](
      s.meld,
      m.m.txs,
      conflictSet,
      m.m.finalized.map(_.accepted).getOrElse(Set()),
      m.m.finalized.map(_.rejected).getOrElse(Set()),
      exeEngine.conflicts,
      exeEngine.depends
    )
    val lazoME = Block.toLazoM(m.m).computeExtended(s.lazo)
    val offOptT = validateMessage(m.id, m.m, s, exeEngine).swap.toOption

    // invalid messages do not participate in merge and are not accounted for double spend guard
    val offCase = offOptT.map(off => ReplayResult(lazoME, none[MeldM[T]], none[GardM[M, T]], off.some))
    // if msg is valid - Meld state and Gard state should be updated
    val validCase = mkMeld.map(_.some).map(ReplayResult(lazoME, _, Block.toGardM(m.m).some, none[Offence]))

    IncompleteMessageInProcessQueue.raiseError.whenA(!complete) >> offCase.getOrElseF(validCase)
  }

  /** Attempt to add message to processor. */
  def add[F[_]: Sync, M, S, T: Ordering](
    m: Block.WithId[M, S, T],
    pStRef: Ref[F, Weaver[M, S, T]],
    stRef: Ref[F, ST[M]],
    exeEngine: ExeEngine[F, M, S, T]
  ): F[Either[Ignored, AddResult[M, S, T]]] = Sync[F].bracket(stRef.modify(_.begin(m.id))) {
    case false => ignoredDuplicate.asLeft[AddResult[M, S, T]].pure[F]
    case true =>
      pStRef
        .modify { _.start(m.id, m.m.minGenJs, m.m.sender) }
        .flatMap {
          case Some(s) =>
            replay(m, s, exeEngine)
              .flatMap { case ReplayResult(lazoME, meldMOpt, gardMOpt, offenceOpt) =>
                pStRef.modify { curSt =>
                  val (newSt, result) = curSt.add(m.id, lazoME, meldMOpt, gardMOpt, offenceOpt, m.m.finalized)
                  (newSt, result)
                }
              }
              .map(_.asRight[Ignored])
          case None =>
            ignoredDetached.asLeft[AddResult[M, S, T]].pure[F]
        }
  }(_ => stRef.modify(_.done(m.id)))

  def apply[F[_]: Async, M, S, T: Ordering](
    pStRef: Ref[F, Weaver[M, S, T]],
    exeEngine: ExeEngine[F, M, S, T],
    threadsNum: Int
  ): F[Processor[F, M, S, T]] =
    for {
      stRef <- Ref.of[F, ST[M]](ST(Set.empty[M]))
      procQueue <- Channel.unbounded[F, Block.WithId[M, S, T]]
    } yield {
      val out = procQueue.stream.parEvalMap(threadsNum) { m =>
        add[F, M, S, T](m, pStRef, stRef, exeEngine).map(m.id -> _)
      }

      new Processor(stRef, procQueue.send(_).void, out.collect { case (m, Right(x)) => m -> x })
    }

  trait Ignored
  final case object IgnoredDuplicate extends Ignored
  final case object IgnoredDetached extends Ignored

  val ignoredDuplicate: Ignored = IgnoredDuplicate
  val ignoredDetached: Ignored = IgnoredDetached
}
