package weaver

import weaver.data.ConflictResolution

trait Offence

object Offence {

  // Message computes invalid fringe
  final case class InvalidFringe[M](shouldBe: Set[M], is: Set[M]) extends Offence

  // Message computes invalid conflict resolution.
  final case class InvalidFringeResolve[T](shouldBe: ConflictResolution[T], is: ConflictResolution[T]) extends Offence

  // Message computes invalid final state for the fringe.
  final case class InvalidFringeState() extends Offence

  // Message tries to invoke double spend.
  // This means put in the list of transactions one that is prohibited due to expiration
  // or one that is observed by the message
  final case class InvalidDoubleSpend[T](doubleSpends: Set[T]) extends Offence

  // Message computes invalid conflict resolution of the conflict set
  final case class InvalidResolution[T](shouldBe: Set[T]) extends Offence

  // Message computes invalid blockchain state hash
  final case class InvalidExec() extends Offence

  def iexec: Offence = InvalidExec()

}
