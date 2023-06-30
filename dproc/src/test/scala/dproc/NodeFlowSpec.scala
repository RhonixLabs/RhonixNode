package dproc

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats.StubbingOpsCats
import org.mockito.matchers.MacroBasedMatchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NodeFlowSpec extends AnyFlatSpec with Matchers with IdiomaticMockito with MacroBasedMatchers {

  val events = mock[NodeFlow.Effects[IO, Int, Int, Int, Int, Int]]
  events.store(0) returnsF ()
  events.triggerProcess(0) returnsF ()
  events.bufferIncoming(0) returnsF ()
  events.added(0, 0) returnsF ()
  events.triggerPropose returnsF ()

  val inputs: Stream[IO, Int]           = Stream(0).repeat.covary[IO]
  val processed: Stream[IO, (Int, Int)] = Stream(0 -> 0).repeat.covary[IO]
  val txs: Stream[IO, Int]              = Stream(0).repeat.covary[IO]
  val proposals: Stream[IO, Int]        = Stream(0).repeat.covary[IO]

  "stream" should "invoke events properly" in {
    val s = NodeFlow[IO, Int, Int, Int, Int, Int](
      inputs.take(1000),
      processed.take(10),
      txs.take(11),
      proposals.take(12),
      events,
      identity,
    )
    s.compile.drain.unsafeRunSync()

    // send to buffer each input item
    events.bufferIncoming(0) wasCalled 1000.times

    // store and trigger process for each block proposed
    events.store(0) wasCalled 12.times
    events.triggerProcess(0) wasCalled 12.times

    // notify buffer about completion and report for each item processed
    events.added(0, 0) wasCalled 10.times

    // trigger propose after each processed and each transaction
    events.triggerPropose wasCalled 21.times
    verifyNoMoreInteractions(events);
  }
}
