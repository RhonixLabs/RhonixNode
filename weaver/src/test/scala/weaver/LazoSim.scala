package weaver

import cats.Monad
import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.rhonix.dagbuilder.BlockDagSchedule.DagNode
import io.rhonix.dagbuilder.{BlockDagBuilder, BlockDagSchedule, NetworkScheduleGen}
import org.scalatest.flatspec.AnyFlatSpec
import weaver.BlockRender.renderLazoMessage
import weaver.data.{Bonds, LazoE, LazoF, LazoM}
import weaver.syntax.all._

class LazoSim extends AnyFlatSpec {
  it should "work" in {
    import cats.effect.unsafe.implicits.global
    LazoSim().unsafeRunSync()
  }
}

object LazoSim {

  final case class LazoDagBuilder[F[_]: Monad, M, S](
    finST: Ref[F, Map[S, LazoF[M]]],
    lazoRef: Ref[F, Lazo[M, S]]
  ) extends BlockDagBuilder[F, M, S] {

    // This Dag builder is valid for the purpose of the simulation when the BlockDagSchedule emits concurrent
    // blocks that are safe to be processed in parallel and there is a guarantee that there is not dependency
    // unprocessed or dependencies between DagNodes emitted concurrently.
    // So this implementation assumes that all dependencies of an input node are already added.
    override def add(node: DagNode[M, S]): F[Unit] = for {
      // compute message data
      lazo <- lazoRef.get
      mgjs = lazo.mgjs(node.js.values.toSet)
      finality = lazo.finality(mgjs)
      offences = Set.empty[M] // TODO generate offences
      lazoM = LazoM[M, S](
        node.sender,
        mgjs,
        offences,
        finality,
        lazo.trustAssumption
      )
      lazoME = lazoM.computeExtended(lazo)
      offenceOpt = none[Offence] // TODO check if message is offence

      // add message to Lazo state
      _ <- lazoRef.modify(_.add(node.id, lazoME, offenceOpt))
      // update finality view for a sender
      _ <- finST.update(_.updated(node.sender, finality))
    } yield ()
  }

  def apply(): IO[Unit] = {
    // get random DAG schedule
    val dagSchedule = BlockDagSchedule[IO, String, String](NetworkScheduleGen.randomWithId(30, 5).sample.get)
    val genesisState = LazoE(Bonds(dagSchedule.senders.map(_ -> 100L).toMap), Int.MaxValue, Int.MaxValue)
    val lazoRef = Ref.unsafe[IO, Lazo[String, String]](Lazo.empty(genesisState))
    val finalityRef = Ref.unsafe[IO, Map[String, LazoF[String]]](Map.empty)
    val dagBuilder = LazoDagBuilder(finalityRef, lazoRef)
    val renderRandomTip = for {
      lazo <- lazoRef.get
      tip = lazo.latestMessages.head
      _ <- renderLazoMessage[IO, String, String](lazo, tip)
    } yield ()
    BlockDagBuilder.build(dagSchedule, dagBuilder) >> renderRandomTip
  }
}
