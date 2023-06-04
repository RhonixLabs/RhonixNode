package weaver.syntax

import cats.data.Kleisli
import weaver.Lazo
import weaver.data.{Bonds, LazoF}
import cats.syntax.all._
import weaver.rules.Dag.{computeFJS, computeMGJS}
import weaver.rules.Finality

trait LazoSTSyntax {
  implicit final def lazoLazoSyntax[M, S](st: Lazo[M, S]): LazoSTOps[M, S] = new LazoSTOps[M, S](st)
}

final class LazoSTOps[M, S](private val s: Lazo[M, S]) {
  // idx of the latest fringe across set of messages
  def lfIdxOpt: Set[M] => Option[Int] = _.map(s.dagData(_: M).fringeIdx).lastOption

  def latestFringe: Set[M] => LazoF[M] =
    _.map(s.dagData(_: M).fringeIdx).lastOption.map(s.fringes).map(LazoF(_)).getOrElse(LazoF.empty[M])

  // latest bonds map for a view defined by mgjs
  def bondsMap(mgjs: Set[M]): Option[Bonds[S]] =
    Kleisli(lfIdxOpt)
      .andThen(Kleisli[Option, Int, Bonds[S]](s.exeData(_: Int).bondsMap.some))
      .run(mgjs)

  def bondsMapUnsafe(mgjs: Set[M]): Bonds[S] = {
    val x = bondsMap(mgjs)
    assert(x.nonEmpty, "Messages of minimal generative set is missing from the state.")
    x.get
  }

  def fjsOpt(mgjs: Set[M]): Option[Set[M]] = bondsMap(mgjs).map(_.activeSet).map(fullJs(_)(mgjs))

  // derive full justification set from mgjs
  def fullJs(activeSet: Set[S]): Set[M] => Set[M] =
    (mgjs: Set[M]) =>
      computeFJS(
        mgjs,
        activeSet,
        s.dagData(_: M).jss,
        (x: M, y: M) => s.seenMap.get(x).exists(_.contains(y)),
        s.dagData(_: M).sender
      )

  def mgjs(fullJs: Set[M]): Set[M] = computeMGJS(fullJs, (x: M, y: M) => s.seenMap.get(x).exists(_.contains(y)))

  def selfJOpt(mgjs: Set[M], sender: S): Option[M] =
    bondsMap(mgjs).map(_.activeSet).flatMap(x => selfJOpt(x, sender, mgjs))

  // self justification option
  def selfJOpt(activeSet: Set[S], sender: S, mgjs: Set[M]): Option[M] =
    fullJs(activeSet).andThen(x => x.find(s.dagData(_).sender == sender))(mgjs)

  // all messages seen through view defined by mgjs
  def view: Set[M] => Set[M] = (mgjs: Set[M]) => mgjs ++ mgjs.flatMap(s.seenMap)

  def finality(mgjs: Set[M]): LazoF[M] = Finality.tryAdvance(mgjs, s).getOrElse(latestFringe(mgjs))

  def conflictSet(m: M): Set[M] = {
    val data = s.dagData(m)
    val fringe = s.fringes(data.fringeIdx)
    view(data.mgjs) -- view(fringe)
  }

  def finalSet(m: M): Set[M] = {
    val data = s.dagData(m)
    val fringe = s.fringes(data.fringeIdx)
    if (fringe.isEmpty) Set()
    else {
      val scopeBottomOpt = fringe.flatMap(s.selfChildMap).map(s.dagData(_).fringeIdx).minOption.flatMap(s.fringes.get)
      assert(scopeBottomOpt.isDefined, "Error: no self children found for the fringe.")
      view(fringe) -- view(scopeBottomOpt.get)
    }
  }

  def scope(m: M): Set[M] = finalSet(m) ++ conflictSet(m) + m
}
