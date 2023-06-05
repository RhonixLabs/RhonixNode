package dproc

import cats.Applicative
import cats.data.Chain
import cats.effect.{Concurrent, Ref, Resource, Sync}
import cats.syntax.all._
import weaver.Lazo
import weaver.data.LazoM

object Debug {
  type TraceState[F[_]] = Ref[F, Chain[String]]
  type TraceF[F[_]] = String => F[Unit]

  def mkLog[F[_]: Concurrent]: F[TraceState[F]] =
    Ref.of[F, Chain[String]](Chain.empty)

  def mkAppender[F[_], M](log: TraceState[F]): TraceF[F] =
    entry => log.update(chain => chain.append(entry))

  def collectLog[F[_]](log: TraceState[F]): F[Chain[String]] = log.get

  def noTraceResource[F[_]: Applicative, M]: Resource[F, TraceF[F]] =
    Resource.make(((_: String) => ().pure[F]).pure[F])(_ => ().pure[F])

  def tracerResource[F[_]: Concurrent, S, M](
    processId: S,
    setTraceId: F[M],
    inbound: Boolean
  ): Resource[F, TraceF[F]] =
    Resource
      .make(mkLog) { x =>
        for {
          log <- collectLog(x)
          x <- setTraceId
        } yield println(
          s"\nTrace for ($inbound) $x @ $processId: \n${log.toList.mkString("\n")}"
        )
      }
      .map(mkAppender)

  def checkForInvalidFringe[F[_]: Sync, M, S](lazo: Lazo[M, S], lazoM: LazoM[M, S]) = {
    val errMsg =
      s"Error. Latest fringe ${lazo.fringes.lastOption} sees new fringes ${lazoM.finality.fFringe}"
    new Exception(errMsg)
      .raiseError[F, Unit]
      .unlessA(
        lazo.fringes.lastOption
          .map { case (_, msgs) => msgs.flatMap(lazo.seenMap) }
          .getOrElse(Set())
          .diff(lazoM.finality.fFringe)
          .intersect(lazoM.finality.fFringe)
          .isEmpty
      )
  }
}
