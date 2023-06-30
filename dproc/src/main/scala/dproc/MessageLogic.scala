package dproc

import cats.Applicative
import cats.data.EitherT
import cats.effect.Sync
import cats.syntax.all.*
import dproc.data.Block
import sdk.syntax.all.*
import weaver.*
import weaver.Gard.GardM
import weaver.Offence.*
import weaver.Weaver.ExeEngine
import weaver.data.*
import weaver.rules.*
import weaver.syntax.all.*

/** Logic of creating and validating messages. */
object MessageLogic {

  /** Weaver state with transactions. */
  final case class WeaverWithTxs[M, S, T](weaver: Weaver[M, S, T], txs: Set[T])

  def computeFringe[F[_]: Sync, M, S](
    minGenJs: Set[M],
    lazo: Lazo[M, S],
  ): F[LazoF[M]] = Sync[F].delay {
    Finality.tryAdvance(minGenJs, lazo).getOrElse(lazo.latestFringe(minGenJs))
  }

  def computeFsResolve[F[_]: Sync, M, S, T: Ordering](
    fFringe: Set[M],
    minGenJs: Set[M],
    s: Weaver[M, S, T],
  ): F[ConflictResolution[T]] =
    for {
      pf <- Sync[F].delay(
              minGenJs
                .map(s.lazo.dagData(_).fringeIdx)
                .toList
                .sorted
                .lastOption
                .map(s.lazo.fringes)
                .getOrElse(Set.empty[M]),
            )
      x  <- Sync[F].delay(Dag.between(fFringe, pf, s.lazo.seenMap))
      r  <- Sync[F].delay(Meld.resolve(x -- s.lazo.offences, s.meld))
    } yield r

  def computeGard[F[_]: Sync, M, T](
    txs: List[T],
    fFringe: Set[M],
    gard: Gard[M, T],
    expT: Int,
  ): F[List[T]] =
    Sync[F].delay(txs.filterNot(gard.isDoubleSpend(_, fFringe, expT)))

  def computeCsResolve[F[_]: Sync, M, S, T: Ordering](
    minGenJs: Set[M],
    fFringe: Set[M],
    lazo: Lazo[M, S],
    meld: Meld[M, T],
  ): F[Set[T]] =
    for {
      x <- Sync[F].delay(Dag.between(minGenJs, fFringe, lazo.seenMap))
      r <- Sync[F].delay(Meld.resolve(x -- lazo.offences, meld))
    } yield r.accepted

  def validateBasic[F[_]: Sync, M, S, T](
    m: Block[M, S, T],
    s: Weaver[M, S, T],
  ): EitherT[F, InvalidBasic, Unit] = {
    val x = Sync[F].delay(Lazo.checkBasicRules(Block.toLazoM(m), s.lazo))
    EitherT(x.map(_.toLeft(())))
  }

  def validateFringe[F[_]: Sync, M, S, T](
    m: Block[M, S, T],
    s: Weaver[M, S, T],
  ): EitherT[F, InvalidFringe[M], Set[M]] =
    EitherT(computeFringe(m.minGenJs, s.lazo).map { case LazoF(fFringe) =>
      (fFringe != m.finalFringe)
        .guard[Option]
        .as(InvalidFringe(fFringe, m.finalFringe))
        .toLeft(m.finalFringe)
    })

  def validateFsResolve[F[_]: Sync, M, S, T: Ordering](
    m: Block[M, S, T],
    s: Weaver[M, S, T],
  ): EitherT[F, InvalidFringeResolve[T], ConflictResolution[T]] =
    EitherT(computeFsResolve(m.finalFringe, m.minGenJs, s).map { finalization =>
      (finalization != m.finalized.getOrElse(ConflictResolution.empty[T]))
        .guard[Option]
        .as(InvalidFringeResolve(finalization, m.finalized.getOrElse(ConflictResolution.empty[T])))
        .toLeft(finalization)
    })

  def validateGard[F[_]: Sync, M, S, T](
    m: Block[M, S, T],
    s: Weaver[M, S, T],
    expT: Int,
  ): EitherT[F, InvalidDoubleSpend[T], Unit] =
    EitherT(computeGard(m.txs, m.finalFringe, s.gard, expT).map { txToPut =>
      (txToPut != m.txs).guard[Option].as(InvalidDoubleSpend(m.txs.toSet -- txToPut)).toLeft(())
    })

  def validateCsResolve[F[_]: Sync, M, S, T: Ordering](
    m: Block[M, S, T],
    s: Weaver[M, S, T],
  ): EitherT[F, InvalidResolution[T], Set[T]] =
    EitherT(computeCsResolve(m.minGenJs, m.finalFringe, s.lazo, s.meld).map { merge =>
      (merge != m.merge).guard[Option].as(InvalidResolution(merge)).toLeft(merge)
    })

  def validateExeData[F[_]: Applicative](
    x: LazoE[?],
    ref: LazoE[?],
  ): EitherT[F, InvalidFringeState, Unit] =
    EitherT.fromOption((x == ref).guard[Option], InvalidFringeState())

  def createMessage[F[_]: Sync, M, S, T: Ordering](
    txs: List[T],
    sender: S,
    state: Weaver[M, S, T],
    exeEngine: ExeEngine[F, M, S, T],
  ): F[Block[M, S, T]] = {
    val mgjs     = state.lazo.latestMGJs
    val offences = state.lazo.offences
    for {
      lazoF   <- computeFringe(mgjs, state.lazo)
      newF     = (lazoF.fFringe != state.lazo.latestFringe(mgjs)).guard[Option]
      fin     <- newF.traverse(_ => computeFsResolve(lazoF.fFringe, mgjs, state))
      lazoE   <- exeEngine.lazo.finalData(lazoF.fFringe)
      txToPut <- computeGard(txs, lazoF.fFringe, state.gard, lazoE.expirationThreshold)
      toMerge <- computeCsResolve(mgjs, lazoF.fFringe, state.lazo, state.meld)
    } yield Block(
      sender = sender,
      minGenJs = mgjs,
      txs = txToPut,
      offences = offences,
      finalFringe = lazoF.fFringe,
      finalized = fin,
      merge = toMerge,
      bonds = lazoE.bonds,
      lazTol = lazoE.lazinessTolerance,
      expThresh = lazoE.expirationThreshold,
    )
  }

  def validateMessage[F[_]: Sync, M, S, T: Ordering](
    id: M,
    m: Block[M, S, T],
    s: Weaver[M, S, T],
    exeEngine: ExeEngine[F, M, S, T],
  ): EitherT[F, Offence, Unit] =
    for {
      _  <- validateBasic(m, s)
      fr <- validateFringe(m, s)
      _  <- validateFsResolve(m, s)
      lE <- EitherT.liftF(exeEngine.lazo.finalData(fr))
      _  <- validateExeData(lE, Block.toLazoE(m))
      _  <- validateGard(m, s, lE.expirationThreshold)
      _  <- validateCsResolve(m, s)
      _  <- EitherT(exeEngine.lazo.replay(id).map(_.guard[Option].toRight(Offence.iexec)))
    } yield ()

  def createBlockWithId[F[_]: Sync, M, S, T: Ordering](
    sender: S,
    sWt: WeaverWithTxs[M, S, T],
    exeEngine: ExeEngine[F, M, S, T],
    idGen: Block[M, S, T] => F[M],
  ): F[Block.WithId[M, S, T]] = for {
    // create message
    m <- MessageLogic.createMessage[F, M, S, T](sWt.txs.toList, sender, sWt.weaver, exeEngine)
    // assign ID (hashing / signing done here)
    b <- idGen(m).map(id => Block.WithId(id, m))
  } yield b

  final case class ReplayResult[M, S, T](
    lazoME: LazoM.Extended[M, S],
    meldMOpt: Option[MeldM[T]],
    gardMOpt: Option[GardM[M, T]],
    offenceOpt: Option[Offence],
  )

  def replay[F[_]: Sync, M, S, T: Ordering](
    m: Block.WithId[M, S, T],
    s: Weaver[M, S, T],
    exeEngine: ExeEngine[F, M, S, T],
  ): F[ReplayResult[M, S, T]] = Sync[F].defer {
    lazy val conflictSet = Dag.between(m.m.minGenJs, m.m.finalFringe, s.lazo.seenMap).flatMap(s.meld.txsMap).toList
    lazy val unseen      = s.lazo.dagData.keySet -- s.lazo.view(m.m.minGenJs)
    lazy val mkMeld      = Meld
      .computeRelationMaps[F, T](
        m.m.txs,
        unseen.flatMap(s.meld.txsMap).toList,
        conflictSet,
        exeEngine.meld.conflicts,
        exeEngine.meld.depends,
      )
      .map { case (cm, dm) =>
        MeldM(
          m.m.txs,
          conflictSet.toSet,
          cm,
          dm,
          m.m.finalized.map(_.accepted).getOrElse(Set()),
          m.m.finalized.map(_.rejected).getOrElse(Set()),
        )
      }

    val lazoME  = Block.toLazoM(m.m).computeExtended(s.lazo)
    val offOptT = validateMessage(m.id, m.m, s, exeEngine).swap.toOption

    // invalid messages do not participate in merge and are not accounted for double spend guard
    val offCase   = offOptT.map(off => ReplayResult(lazoME, none[MeldM[T]], none[GardM[M, T]], off.some))
    // if msg is valid - Meld state and Gard state should be updated
    val validCase = mkMeld.map(_.some).map(ReplayResult(lazoME, _, Block.toGardM(m.m).some, none[Offence]))

    offCase.getOrElseF(validCase)
  }
}
