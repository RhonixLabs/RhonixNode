package weaver

import weaver.Gard.GardM
import weaver.data.*

/** The state of the process are state supporting all protocols that the process run. */
final case class Weaver[M, S, T](
  lazo: Lazo[M, S],
  meld: Meld[M, T],
  gard: Gard[M, T],
) {
  def add(
    id: M,
    lazoM: LazoM.Extended[M, S],
    meldMOpt: Option[MeldM[T]],
    gardMOpt: Option[GardM[M, T]],
    offenceOpt: Option[Offence],
  ): (Weaver[M, S, T], (Set[M], Boolean)) =
    if (lazo.contains(id)) (this, Set.empty[M] -> false)
    else {
      val (newLazo, gc) = lazo.add(id, lazoM, offenceOpt)
      val newMeld       = meldMOpt.map(meld.add(id, _, gc)).getOrElse(meld)
      val newGard       = gardMOpt.map(gard.add).getOrElse(gard)
      val newSt         = Weaver(newLazo, newMeld, newGard)
      (newSt, gc -> true)
    }
}

object Weaver {
  def empty[M, S, T](trust: LazoE[S]): Weaver[M, S, T] = Weaver(Lazo.empty(trust), Meld.empty, Gard.empty)

  final case class ExeEngine[F[_], M, S, T](lazo: Lazo.ExeEngine[F, M, S], meld: Meld.ExeEngine[F, T])

  def shouldAdd[M, S](lazo: Lazo[M, S], msgId: M, minGenJs: Set[M], sender: S): Boolean =
    // if already added - ignore
    if (lazo.contains(msgId)) false
    // or check with Lazo rules
    else Lazo.canAdd(minGenJs, sender, lazo)
}
