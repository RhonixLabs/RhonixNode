package weaver.data

/**
 * Finality data.
 * @param fFringe final fringe.
 */
final case class LazoF[M](fFringe: Set[M])

object LazoF { def empty[M]: LazoF[M] = LazoF(Set.empty[M]) }
