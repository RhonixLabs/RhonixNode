package weaver.rules

object Ejection {
  // TODO
//
//  // For ejection purposes only N self ancestors are required, therefore queue of length N is enough.
//  type EjectionData = Queue[Set[Int]]
//
//  /**
//    * Compute data supporting ejection logic for the new message
//    * @param selfParentOpt ejection data for the self parent (if exists)
//    * @param partition partition computed for the message
//    * @param lazinessTolerance system parameter, the bigger the number the higher the tolerance for validator laziness.
//    * @return ejection data for the new message
//    */
//  def compute(
//      selfParentOpt: Option[EjectionData],
//      partition: Set[Int],
//      lazinessTolerance: Int
//  ): EjectionData =
//    selfParentOpt
//      .map { sp =>
//        if (sp.size == lazinessTolerance) sp.dequeue._2.enqueue(Set(partition))
//        else sp.dequeue._2
//      }
//      .getOrElse(Queue.empty[Set[Int]])
//
//  def eject(bondsMap: Bonds[S], ejectionData: EjectionData): Set[Int] =
//    bondsMap.activeSet -- ejectionData.iterator.flatten.toSet
}
