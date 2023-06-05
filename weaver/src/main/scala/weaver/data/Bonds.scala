package weaver.data

final case class Bonds[S](private val bonds: Map[S, Long]) extends AnyVal {
  private def totalStake: Double = bonds.values.foldLeft(0d)(_ + _)
  private def stake(senders: Set[S]): Double = {
    val missingSenders = senders -- bonds.keySet
    assert(missingSenders.isEmpty, s"Senders $missingSenders missing in $bonds")
    bonds.filter { case (s, _) => senders.contains(s) }.values.foldLeft(0d)(_ + _)
  }

  def isBonded(sender: S) = bonds.contains(sender)
  def activeSet: Set[S] = bonds.keySet
  def isSuperMajority: Set[S] => Boolean = stake(_: Set[S]) / totalStake > 2d / 3
  def unbond(senders: Set[S]): Bonds[S] = copy(bonds -- senders)
}

object Bonds { def empty[S]: Bonds[S] = Bonds(Map.empty[S, Long]) }
