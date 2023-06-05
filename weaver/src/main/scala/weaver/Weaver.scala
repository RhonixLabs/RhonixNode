package weaver

import cats.syntax.all._
import weaver.Gard.GardM
import weaver.Weaver._
import weaver.data._

/** The state of the process are state supporting all protocols that the process run. */
final case class Weaver[M, S, T](
  lazo: Lazo[M, S],
  meld: Meld[M, T],
  gard: Gard[M, T]
) {

  def start(msgId: M, minGenJs: Set[M], sender: S): (Weaver[M, S, T], Option[Weaver[M, S, T]]) =
    // if already added - ignore
    if (lazo.contains(msgId)) this -> none[Weaver[M, S, T]]
    // if minGenJs is empty - genesis case, process if no final fringe yet found
    else if (minGenJs.isEmpty) this -> lazo.fringes.isEmpty.guard[Option].as(this)
    // if all dependencies are added
    else if (minGenJs.forall(lazo.contains)) {
      val fromOffender = {
        val selfJs = minGenJs.filter(lazo.dagData(_).sender == sender)
        val fromEquivocator = selfJs.size > 1
        lazy val selfJsIsOffence = selfJs.headOption.exists(lazo.offences.contains)
        fromEquivocator || selfJsIsOffence
      }
      // ignore from offenders in the view
      if (fromOffender) this -> none[Weaver[M, S, T]]
      // process otherwise
      else this -> this.some
    } else this -> none[Weaver[M, S, T]]

  def add(
    id: M,
    lazoM: LazoM.Extended[M, S],
    meldMOpt: Option[MeldM[T]],
    gardMOpt: Option[GardM[M, T]],
    offenceOpt: Option[Offence],
    finality: Option[ConflictResolution[T]]
  ): (Weaver[M, S, T], AddResult[M, S, T]) = {
    val (newLazo, gc) = lazo.add(id, lazoM, offenceOpt)
    val newMeld = meldMOpt.map(meld.add(id, _, gc)).getOrElse(meld)
    val newGard = gardMOpt.map(gard.add).getOrElse(gard)
    val newSt = Weaver(newLazo, newMeld, newGard)
    (newSt, AddResult(newSt, gc, finality, offenceOpt))
  }
}

object Weaver {
  def empty[M, S, T](trust: LazoE[S]): Weaver[M, S, T] = Weaver(Lazo.empty(trust), Meld.empty, Gard.empty)

  trait ExeEngine[F[_], M, S, T] extends Lazo.ExeEngine[F, M, S] with Meld.ExeEngine[F, T]

  final case class AddResult[M, S, T](
    newState: Weaver[M, S, T],
    garbage: Set[M],
    finality: Option[ConflictResolution[T]],
    offenceOpt: Option[Offence]
  )
}
