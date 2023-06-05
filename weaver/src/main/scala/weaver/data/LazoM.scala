package weaver.data

/**
  * Minimal necessary data for a protocol message.
  *
  * @param sender creator of the message.
  * @param mgjs minimal generative justification set - define the view that the message agreed on.
  * @param offences messages that the messages disagree with.
  * @param finality finality computed by the message.
  * @param state execution data for the the new final fringe found by the message. Provided by execution engine.
  */
final case class LazoM[M, S](
  sender: S,
  mgjs: Set[M],
  offences: Set[M],
  finality: LazoF[M],
  state: LazoE[S]
)

object LazoM {

  /** Data required by protocol that can be derived from LazoM.
    * Might be computationally expensive so this is on a data type. */
  final case class LazoMExt[M, S](
    fjs: Set[M],
    selfJOpt: Option[M],
    seen: Set[M],
    baseBonds: Bonds[S],
    lfIdx: Option[Int]
  )

  final case class Extended[M, S](
    lazoM: LazoM[M, S],
    ext: LazoMExt[M, S]
  )
}
