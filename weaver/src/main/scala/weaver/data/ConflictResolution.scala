package weaver.data

final case class ConflictResolution[T](accepted: Set[T], rejected: Set[T])
