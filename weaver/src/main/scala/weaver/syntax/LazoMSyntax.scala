package weaver.syntax

import weaver.Lazo
import weaver.data.LazoM.LazoMExt
import weaver.data.{Bonds, LazoM}
import weaver.syntax.all._

trait LazoMSyntax {
  implicit final def lazoMSyntax[M, S](x: LazoM[M, S]): LazoMOps[M, S] = new LazoMOps(x)
}

final class LazoMOps[M, S](private val x: LazoM[M, S]) extends AnyVal {
  def fjs(implicit state: Lazo[M, S]): Set[M] =
    state.fjsOpt(x.mgjs).getOrElse(x.mgjs) // this orElse is for genesis case

  def selfJOpt(implicit state: Lazo[M, S]): Option[M] = state.selfJOpt(x.mgjs, x.sender)

  def seen(implicit state: Lazo[M, S]): Set[M] = state.view(x.mgjs)

  def baseBonds(implicit state: Lazo[M, S]): Bonds[S] =
    state.bondsMap(x.mgjs).getOrElse(x.state.bonds)

  def lfIdx(implicit state: Lazo[M, S]): Option[Int] = state.lfIdxOpt(x.mgjs)

  def computeExtended(state: Lazo[M, S]): LazoM.Extended[M, S] = {
    implicit val s = state
    LazoM.Extended(x, LazoMExt(fjs, selfJOpt, seen, baseBonds, lfIdx))
  }
}
