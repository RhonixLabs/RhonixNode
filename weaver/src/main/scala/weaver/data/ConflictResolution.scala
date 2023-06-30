package weaver.data

final case class ConflictResolution[T](accepted: Set[T], rejected: Set[T])

object ConflictResolution {
  def empty[T]: ConflictResolution[T] = ConflictResolution(Set.empty[T], Set.empty[T])
}
