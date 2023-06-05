package weaver.syntax

import weaver.data._

trait LazoMESyntax {
  implicit final def lazoMESyntax[M, S](x: LazoM.Extended[M, S]): LazoMEOps[M, S] =
    new LazoMEOps(x)
}

final class LazoMEOps[M, S](private val x: LazoM.Extended[M, S]) extends AnyVal {
  def sender: S = x.lazoM.sender

  def mgj: Set[M] = x.lazoM.mgjs

  def offences: Set[M] = x.lazoM.offences

  def fringes: LazoF[M] = x.lazoM.finality

  def state: LazoE[S] = x.lazoM.state

  def fjs: Set[M] = x.ext.fjs

  def selfJOpt: Option[M] = x.ext.selfJOpt

  def seen: Set[M] = x.ext.seen

  def baseBonds: Bonds[S] = x.ext.baseBonds

  def lfIdx: Option[Int] = x.ext.lfIdx
}
