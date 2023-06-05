package weaver

import cats.syntax.all._
import weaver.Lazo._
import weaver.Offence.InvalidFringe
import weaver.data._
import weaver.rules.Dag._
import weaver.rules._
import weaver.syntax.all._

/** State supporting Lazo protocol. */
final case class Lazo[M, S](
  dagData: Map[M, DagData[M, S]], // dag data
  exeData: Map[Int, ExeData[S]], // execution data for all fringes
  fringes: Map[Int, Set[M]], // all final fringes in the scope of Lazo
  fringesR: Map[Set[M], Int], // all final fringes in the scope of Lazo
  seenMap: Map[M, Set[M]], // all messages in the state seen by a message
  selfChildMap: Map[M, Option[M]], // self child (if exists) for a message,
  offences: Set[M], // offences detected
  latestF: Int,
  latestMessages: Set[M],
  woSelfJ: Set[M], // messages without self parent - which means they are from newly bonded
  trustAssumption: LazoE[S] // bonds and other data from the state that is trusted by the node.
  // It us either local bonds file and others or data read from the LFS which node is bootstrapping from
) { self =>
  override def equals(obj: Any): Boolean = obj match {
    case x: Lazo[_, _] => x.latestMessages == this.latestMessages
    case _             => false
  }

  def add(
    id: M,
    m: LazoM.Extended[M, S],
    offenceOpt: Option[Offence]
  ): (Lazo[M, S], Set[M]) = {

    val fringeId = offenceOpt match {
      case Some(InvalidBasic())      => FRINGE_IDX_INVALID
      case Some(InvalidFringe(_, _)) => FRINGE_IDX_INVALID
      case _ =>
        val nextF = latestF + 1
        val nextFringeId = if (nextF == F_ID_MAX) F_ID_MIN else nextF
        fringesR.getOrElse(m.fringes.fFringe, nextFringeId)
    }
    val newFringeIdOpt = (fringeId != FRINGE_IDX_INVALID && !fringes.contains(fringeId))
      .guard[Option]
      .as(fringeId)
    val newLatestF = newFringeIdOpt.getOrElse(latestF)
    val newOffences = offenceOpt.fold(offences)(_ => offences + id)
    // Attempt to prune the state. Laziness tolerance is read from the latest fringe in the
    // state before message adding.
    val (dataToPrune, fringesToPrune) = newFringeIdOpt
      .flatMap(_ => m.lfIdx)
      .map(exeData(_).lazinessTolerance)
      .fold((Set.empty[M], Set.empty[Int]))(prune(this, latestF, _))
    val newDagData = dagData + (id -> DagData(
      m.mgj,
      m.fjs,
      m.offences,
      fringeId,
      m.sender
    )) -- dataToPrune
    val newFringes = newFringeIdOpt.fold(fringes)(fId => fringes + (fId -> m.fringes.fFringe)) -- fringesToPrune
    val newFringesR = newFringeIdOpt.fold(fringesR)(fId => fringesR + (m.fringes.fFringe -> fId)) -- fringesToPrune
      .map(fringes)
    val newSelfChildMap = m.selfJOpt
      .map(sjId => selfChildMap + (id -> none[M]) + (sjId -> id.some))
      .getOrElse(selfChildMap + (id -> none[M])) -- dataToPrune
    // TODO would be good not tp prune each value in a seenMap. But this is not strictly correct because
    //  can lead to a cycle situation when old messages see ids that are assigned to new ones.
    val newSeenMap = seenMap -- dataToPrune + (id -> (m.seen -- dataToPrune))
    val newExeData = exeData + (fringeId -> ExeData(m.state.lazinessTolerance, m.state.bonds)) -- fringesToPrune
    val newLatestMessages = m.selfJOpt.foldLeft(latestMessages + id)(_ - _)
    val newWoSelfJ = m.selfJOpt.fold(woSelfJ + id)(_ => woSelfJ) -- dataToPrune

    val newLazo = copy(
      dagData = newDagData,
      exeData = newExeData,
      fringes = newFringes,
      fringesR = newFringesR,
      seenMap = newSeenMap,
      selfChildMap = newSelfChildMap,
      latestF = newLatestF,
      offences = newOffences,
      latestMessages = newLatestMessages,
      woSelfJ = newWoSelfJ,
      trustAssumption = trustAssumption
    )

    (newLazo, dataToPrune)
  }

  lazy val latestMGJs: Set[M] = computeMGJS(latestMessages, (x: M, y: M) => seenMap.get(x).exists(_.contains(y)))

  def contains(m: M): Boolean = dagData.contains(m)
}

object Lazo {

  /** Data required for Lazo from execution engine. */
  trait ExeEngine[F[_], M, S] {
    // whether message replay was successful
    def replay(m: M): F[Boolean]
    // data read from the final state associated with the final fringe
    def finalData(fringe: Set[M]): F[LazoE[S]]
  }

  // Fringe index for messages that are declared as invalid due to offences that prevent to compute valid fringe
  val FRINGE_IDX_INVALID = Int.MinValue
  val F_ID_MIN = Int.MinValue + 1
  val F_ID_MAX = Int.MaxValue

  /**
   * DAG data about the message.
   *
   * @param mgjs      minimal generative justifications
   * @param jss       justifications can be derived from mgjs and seen map, but it can be costly, so better to store it
   * @param fringeIdx index if the final fringe
   * @param sender    sender
   */
  final case class DagData[M, S](
    mgjs: Set[M],
    jss: Set[M],
    offences: Set[M],
    fringeIdx: Int,
    sender: S
  )

  /** Data required for the protocol that should be provided by the execution engine. */
  final case class ExeData[S](lazinessTolerance: Int, bondsMap: Bonds[S])

  def empty[M, S](initExeData: LazoE[S]): Lazo[M, S] = new Lazo(
    dagData = Map.empty[M, DagData[M, S]],
    exeData = Map.empty[Int, ExeData[S]],
    fringes = Map.empty[Int, Set[M]],
    fringesR = Map.empty[Set[M], Int],
    seenMap = Map.empty[M, Set[M]],
    selfChildMap = Map.empty[M, Option[M]],
    latestF = F_ID_MIN - 1,
    offences = Set(),
    latestMessages = Set(),
    woSelfJ = Set(),
    trustAssumption = initExeData
  )

  /** Prune the state upon finding the new fringe. */
  def prune[M, S](
    state: Lazo[M, S],
    latestFringeIdx: Int,
    lazinessTolerance: Int
  ): (Set[M], Set[Int]) =
    if (latestFringeIdx > F_ID_MIN + lazinessTolerance) {
      // Pruned state won't be able to process any message with fringe below prune fringe.
      val pruneFringeIdx = latestFringeIdx - lazinessTolerance
      val dataToPrune = state.fringes(pruneFringeIdx).flatMap(state.seenMap)
      val fringesToPrune = state.fringes.collect { case (i, _) if i < pruneFringeIdx => i }
      (dataToPrune, fringesToPrune.toSet)
    } else Set.empty[M] -> Set.empty[Int]

  /** Whether message can be added to the state. */
  def canAdd[M, S](minGenJs: Set[M], sender: S, state: Lazo[M, S]): Boolean = {
    // whether all justifications are in the state
    val jsProcessed = minGenJs.forall(state.dagData.contains)
    // genesis case
    lazy val genesisCase = minGenJs.isEmpty && state.fringes.isEmpty
    // if an offender is detected in the view - there should not be any future message added;
    // this is necessary to meet the frugality basic rule.
    lazy val notFromOffender = {
      val selfJs = state.selfJOpt(minGenJs, sender)
      val notFromEquivocator = selfJs.size <= 1
      lazy val selfJsIsValid = selfJs.forall(!state.offences.contains(_))
      notFromEquivocator && selfJsIsValid
    }
    (jsProcessed && notFromOffender) || genesisCase
  }

  /** Validate message for basic rules against the state. */
  def checkBasicRules[M, S](
    m: LazoM[M, S],
    state: Lazo[M, S]
  ): Option[InvalidBasic] = {
    val selfParentOpt = m.selfJOpt(state)
    val latestFIdxOpt = state.lfIdxOpt(m.mgjs)
    val bondsMap = latestFIdxOpt.map(state.exeData(_).bondsMap).getOrElse(m.state.bonds)
    val justifications = computeFJS(
      m.mgjs,
      bondsMap.activeSet,
      state.dagData(_: M).jss,
      (x: M, y: M) => state.seenMap.get(x).exists(_.contains(y)),
      state.dagData(_: M).sender
    )
    val seen = (target: M) => m.mgjs.exists(state.seenMap(_).contains(target))
    val senderF = state.dagData(_: M).sender
    Basic
      .validate(
        justifications,
        m.offences,
        selfParentOpt.map(state.dagData(_).mgjs).getOrElse(Set()),
        selfParentOpt.map(state.dagData(_).offences).getOrElse(Set()),
        justifications.map(j => j -> state.dagData(j).offences).toMap,
        senderF,
        seen
      )
      .toOption
  }
}
