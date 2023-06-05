package dproc

import cats.Applicative
import cats.data.EitherT
import cats.effect.Sync
import cats.syntax.all._
import dproc.data.Block
import weaver.Offence._
import weaver.Weaver.ExeEngine
import weaver.data.{ConflictResolution, LazoE, LazoF}
import weaver.rules._
import weaver.syntax.all._
import weaver._

/** Logic of creating and validating messages. */
object MessageLogic {

  def computeFringe[F[_]: Sync, M, S](
    minGenJs: Set[M],
    lazo: Lazo[M, S]
  ): F[LazoF[M]] = Sync[F].delay { Finality.tryAdvance(minGenJs, lazo).getOrElse(lazo.latestFringe(minGenJs)) }

  def computeFsResolve[F[_]: Sync, M, S, T: Ordering](
    fFringe: Set[M],
    minGenJs: Set[M],
    s: Weaver[M, S, T]
  ): F[ConflictResolution[T]] =
    for {
      pf <- Sync[F].delay(
        minGenJs
          .map(s.lazo.dagData(_).fringeIdx)
          .toList
          .sorted
          .lastOption
          .map(s.lazo.fringes)
          .getOrElse(Set.empty[M])
      )
      x <- Sync[F].delay(Dag.between(fFringe, pf, s.lazo.seenMap))
      r <- Sync[F].delay(Meld.resolve(x, s.meld))
    } yield r

  def computeGard[F[_]: Sync, M, T](
    txs: List[T],
    fFringe: Set[M],
    gard: Gard[M, T],
    expT: Int
  ): F[List[T]] =
    Sync[F].delay(txs.filterNot(gard.isDoubleSpend(_, fFringe, expT)))

  def computeCsResolve[F[_]: Sync, M, S, T: Ordering](
    minGenJs: Set[M],
    fFringe: Set[M],
    lazo: Lazo[M, S],
    meld: Meld[M, T]
  ): F[Set[T]] =
    for {
      x <- Sync[F].delay(Dag.between(minGenJs, fFringe, lazo.seenMap))
      r <- Sync[F].delay(Meld.resolve(x, meld))
    } yield r.accepted

  def validateBasic[F[_]: Sync, M, S, T](
    m: Block[M, S, T],
    s: Weaver[M, S, T]
  ): EitherT[F, InvalidBasic, Unit] = {
    val x = Sync[F].delay(Lazo.checkBasicRules(Block.toLazoM(m), s.lazo))
    EitherT(x.map(_.toLeft(())))
  }

  def validateFringe[F[_]: Sync, M, S, T](
    m: Block[M, S, T],
    s: Weaver[M, S, T]
  ): EitherT[F, InvalidFringe[M], Set[M]] =
    EitherT(computeFringe(m.minGenJs, s.lazo).map { case LazoF(fFringe) =>
      (fFringe != m.finalFringe)
        .guard[Option]
        .as(InvalidFringe(fFringe, m.finalFringe))
        .toLeft(m.finalFringe)
    })

  def validateFsResolve[F[_]: Sync, M, S, T: Ordering](
    m: Block[M, S, T],
    s: Weaver[M, S, T]
  ): EitherT[F, InvalidFringeResolve[T], ConflictResolution[T]] =
    EitherT(computeFsResolve(m.finalFringe, m.minGenJs, s).map { finalization =>
      (finalization.accepted != m.finalized)
        .guard[Option]
        .as(InvalidFringeResolve(finalization.accepted))
        .toLeft(finalization)
    })

  def validateGard[F[_]: Sync, M, S, T](
    m: Block[M, S, T],
    s: Weaver[M, S, T],
    expT: Int
  ): EitherT[F, InvalidDoubleSpend[T], Unit] =
    EitherT(computeGard(m.txs, m.finalFringe, s.gard, expT).map { txToPut =>
      (txToPut != m.txs).guard[Option].as(InvalidDoubleSpend(m.txs.toSet -- txToPut)).toLeft(())
    })

  def validateCsResolve[F[_]: Sync, M, S, T: Ordering](
    m: Block[M, S, T],
    s: Weaver[M, S, T]
  ): EitherT[F, InvalidResolution[T], Set[T]] =
    EitherT(computeCsResolve(m.minGenJs, m.finalFringe, s.lazo, s.meld).map { merge =>
      (merge != m.merge).guard[Option].as(InvalidResolution(merge)).toLeft(merge)
    })

  def validateExeData[F[_]: Applicative](
    x: LazoE[?],
    ref: LazoE[?]
  ): EitherT[F, InvalidFringeState, Unit] =
    EitherT.fromOption((x == ref).guard[Option], InvalidFringeState())

  def createMessage[F[_]: Sync, M, S, T: Ordering](
    txs: List[T],
    sender: S,
    state: Weaver[M, S, T],
    exeEngine: ExeEngine[F, M, S, T]
  ): F[Block[M, S, T]] = {
    val mgjs = state.lazo.latestMGJs
    val offences = state.lazo.offences
    for {
      lazoF <- computeFringe(mgjs, state.lazo)
      newF = (lazoF.fFringe == state.lazo.fringes(state.lazo.latestF)).guard[Option]
      fin <- newF.traverse(_ => computeFsResolve(lazoF.fFringe, mgjs, state))
      lazoE <- exeEngine.finalData(lazoF.fFringe)
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
      expThresh = lazoE.expirationThreshold
    )
  }

  def validateMessage[F[_]: Sync, M, S, T: Ordering](
    id: M,
    m: Block[M, S, T],
    s: Weaver[M, S, T],
    exeEngine: ExeEngine[F, M, S, T]
  ): EitherT[F, Offence, Unit] =
    for {
      _ <- validateBasic(m, s)
      fr <- validateFringe(m, s)
      _ <- validateFsResolve(m, s)
      lE <- EitherT.liftF(exeEngine.finalData(fr))
      _ <- validateExeData(lE, Block.toLazoE(m))
      _ <- validateGard(m, s, lE.expirationThreshold)
      _ <- validateCsResolve(m, s)
      _ <- EitherT(exeEngine.replay(id).map(_.guard[Option].toRight(Offence.iexec)))
    } yield ()
}
