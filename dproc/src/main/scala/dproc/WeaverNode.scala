package dproc

import cats.effect.Ref
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import dproc.MessageLogic.{createBlockWithId, WeaverWithTxs}
import dproc.data.Block
import fs2.Stream
import sdk.node.{Processor, Proposer}
import sdk.syntax.all.logTimeF
import weaver.Weaver.ExeEngine
import weaver.data.ConflictResolution
import weaver.{Offence, Weaver}

object WeaverNode {

  final case class AddEffect[M, T](
    garbage: Set[M],
    finalityOpt: Option[ConflictResolution[T]],
    offenceOpt: Option[Offence],
  )

  /**
   * Replay block and add it to the Weaver state.
   */
  def replayAndAdd[F[_]: Sync, M, S, T: Ordering](
    b: Block.WithId[M, S, T],
    weaverStRef: Ref[F, Weaver[M, S, T]],
    exeEngine: ExeEngine[F, M, S, T],
  ): F[AddEffect[M, T]] =
    for {
      w  <- weaverStRef.get
      br <- MessageLogic.replay(b, w, exeEngine)
      r  <- weaverStRef.modify(_.add(b.id, br.lazoME, br.meldMOpt, br.gardMOpt, br.offenceOpt))
      _  <- new Exception(s"Add failed after replay which should not be possible.").raiseError.unlessA(r._2)
    } yield AddEffect(r._1, b.m.finalized, br.offenceOpt)

  /**
   * Whether block should be added to the state enclosed in Ref.
   *
   * This should be called when full block is received before attempting to add.
   */
  def shouldAdd[F[_]: Sync, M, S, T](
    b: Block.WithId[M, S, T],
    weaverStRef: Ref[F, Weaver[M, S, T]],
  ): F[Boolean] = weaverStRef.get.map(w => Weaver.shouldAdd(w.lazo, b.id, b.m.minGenJs ++ b.m.offences, b.m.sender))

  /**
   * Stream of proposals with callback to trigger propose
   */
  def propWithTrig[F[_]: Async, M, S, T: Ordering](
    idOpt: Option[S],
    proposerStRef: Ref[F, Proposer.ST],
    weaverStRef: Ref[F, Weaver[M, S, T]],
    readTxs: => F[Set[T]],
    exeEngine: ExeEngine[F, M, S, T],
    idGen: Block[M, S, T] => F[M],
  ): F[(Stream[F, Block.WithId[M, S, T]], F[Unit])] = idOpt
    .traverse { id =>
      val proposeF = proposeOnLatest(id, weaverStRef, readTxs, exeEngine, idGen)
      Proposer[F, Block.WithId[M, S, T]](proposerStRef, proposeF)
    }
    .map(_.getOrElse((Stream.empty, Sync[F].unit)))

  def procWithTrig[F[_]: Async, M, S, T: Ordering](
    weaverStRef: Ref[F, Weaver[M, S, T]],
    exeEngine: ExeEngine[F, M, S, T],
    processorStRef: Ref[F, Processor.ST[M]],
    loadBlock: M => F[Block.WithId[M, S, T]],
  ): F[(Stream[F, (M, AddEffect[M, T])], M => F[Unit])] = {
    def processF(m: M): F[Option[AddEffect[M, T]]] = for {
      b  <- loadBlock(m)
      go <- shouldAdd(b, weaverStRef)
      r  <- go.guard[Option].traverse(_ => replayAndAdd(b, weaverStRef, exeEngine))
    } yield r

    Processor[F, M, Option[AddEffect[M, T]]](processorStRef, processF).map { case (s, c) =>
      s.collect { case (m, Some(r)) => m -> r } -> c
    }
  }

  def proposeOnLatest[F[_]: Sync, M, S, T: Ordering](
    sender: S,
    weaverStRef: Ref[F, Weaver[M, S, T]],
    readTxs: => F[Set[T]],
    exeEngine: ExeEngine[F, M, S, T],
    idGen: Block[M, S, T] => F[M],
  ): F[Block.WithId[M, S, T]] = (weaverStRef.get, readTxs)
    .mapN(WeaverWithTxs(_, _))
    .flatMap(createBlockWithId(sender, _, exeEngine, idGen))
}
