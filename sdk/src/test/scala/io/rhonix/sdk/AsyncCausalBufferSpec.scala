package io.rhonix.sdk

import cats.Parallel
import cats.effect.std.Random
import cats.effect.{Async, IO, Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.rhonix.dagbuilder.BlockDagSchedule.DagNode
import io.rhonix.dagbuilder.{BlockDagBuilder, BlockDagSchedule, NetworkScheduleGen}
import io.rhonix.sdk.AsyncCausalBufferSpec.causalOrderTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
class AsyncCausalBufferSpec extends AnyFlatSpec with Matchers {
  "Buffer" should "output all message supplied to the input AND output in causal order" in {
    import cats.effect.unsafe.implicits.global
    implicit val rnd: Random[IO] = Random.scalaUtilRandom[IO].unsafeRunSync()
    val test = causalOrderTest[IO].unsafeRunSync()

    // everything supplied to input should be in output
    test.input shouldBe test.output.keySet
    // for all values emitted all dependencies have to be in the state
    test.output.values.forall(_ == true) shouldBe true
  }
}

object AsyncCausalBufferSpec {

  /** Dag builder that just accumulates nodes in a set. */
  final case class NoOpDagBuilder[F[_], M, S](stRef: Ref[F, Set[DagNode[M, S]]]) extends BlockDagBuilder[F, M, S] {
    override def add(node: DagNode[M, S]): F[Unit] = stRef.update(_ + node)
  }

  final case class TestResult(input: Set[String], output: Map[String, Boolean])
  def causalOrderTest[F[_]: Async: Parallel: Random]: F[TestResult] = {
    for {
      // prepare random dag schedule = 50 senders, 300 rounds
      networkSchedule <- Async[F].delay(NetworkScheduleGen.randomWithId(300, 50).sample.get)
      dagSchedule = BlockDagSchedule[F, String, String](networkSchedule)

      // build dag, for this test just a populating set of blocks is enough so NoOpDagBuilder is used
      stRef <- Ref[F].of(Set.empty[DagNode[String, String]])
      _ <- BlockDagBuilder.build(dagSchedule, NoOpDagBuilder(stRef))

      // all blocks in the DAG
      dagNodes <- stRef.get.map(_.map(x => x.id -> x)).map(_.toMap)
      // stream of blocks of the DAG in some random order
      blocksStream = Stream.fromIterator(dagNodes.iterator.map(_._1), 1)

      // create buffer
      buffer <- AsyncCausalBuffer[F, String]()

      // create mock of a state that is going to be an upstream for the messages from buffer.
      // For the purpose of the test it is enough to just have a set
      state <- Ref[F].of(Set.empty[String])

      // for each item emitted dependencies of an item should be in the state (so emitted earlier)
      dependenciesSatisfiedF = (m: Set[String]) => state.get.map(x => m.intersect(x))
      jsF = (m: String) => dagNodes(m).js.values.toSet
      test = blocksStream
        .through(buffer.pipe(jsF, dependenciesSatisfiedF, 100))
        .evalMap { x =>
          // This mocks replay process, takes from 0 to 100 milliseconds
          val replay = Random[F].betweenInt(0, 100).map(x => Temporal[F].sleep(x.millis))
          // Updating a dag after replay
          val updateDAG = state.update(_ + x)
          // This check is the purpose of this test - all dependencies have to be in the state when stream
          // emits an item, whatever the order of input is
          val check = state.get.map(s => jsF(x).diff(s).isEmpty)
          check.map(x -> _) <* (replay >> updateDAG >> buffer.complete(Set(x)))
        }

      out <- test.compile.to(Map)
      // all input items should be emitted to output AND emitted in causal order
    } yield TestResult(dagNodes.keySet, out)
  }
}
