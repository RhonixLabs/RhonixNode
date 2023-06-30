package sdk.node

import cats.effect.kernel.Async
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref, Sync}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sdk.node.Processor.*

import scala.concurrent.duration.DurationInt

class ProcessorEffectsSpec extends AnyFlatSpec with Matchers {

  behavior of "processCase"

  val ok        = IO.unit
  val cancelled = IO.canceled
  val failed    = IO.raiseError[Unit](new Exception())

  def processAndGet[F[_]: Async](f: => F[Unit]): F[(Set[Int], Vector[Int])] = for {
    stRef <- Ref.of[F, ST[Int]](Processor.default())
    _     <- Sync[F].start(processCase[F, Int, Unit](stRef, _ => f).run(0)).flatMap(_.join)
    state <- stRef.get
  } yield state.processingSet -> state.waitingList

  "when ok" should "remove from processing set" in {
    val (processing, waiting) = processAndGet[IO](ok).unsafeRunSync()
    processing shouldBe Set.empty
    waiting shouldBe Vector.empty
  }

  "when cancelled" should "remove from processing set and put in waiting set" in {
    val (processing, waiting) = processAndGet[IO](cancelled).unsafeRunSync()
    processing shouldBe Set.empty
    waiting shouldBe Vector(0)
  }

  "when failed" should "remove from processing set and put in waiting set" in {
    val (processing, waiting) = processAndGet[IO](failed).unsafeRunSync()
    processing shouldBe Set.empty
    waiting shouldBe Vector(0)
  }

  it should "return value as right projection if process succeed" in {
    for {
      stRef <- Ref.of[IO, ST[Int]](Processor.default())
      res   <- processCase[IO, Int, Int](stRef, IO.delay(_)).run(0)
    } yield res shouldBe Right(0)
  }

  behavior of "processor"

  def output[F[_]: Async](initSt: ST[Int], inputLength: Int, f: Int => F[Int]) = Stream.force(for {
    stRef <- SignallingRef.of[F, ST[Int]](initSt)
    r     <- Processor[F, Int, Int](stRef, f)
  } yield {
    val input         = 0 until inputLength
    val (p, callback) = r
    p.take(Math.max(1, inputLength).toLong).map(_._2) concurrently
      Stream.emits(input.toList).parEvalMapUnorderedUnbounded(callback)
  })

  val initSt = Processor.default[Int]()

  it should "output all input items" in {
    val out = output(initSt, 99, (x: Int) => IO.delay(x))
    out.compile.to(Set).unsafeRunSync() shouldBe (0 until 99).toSet
  }

  val elementProcessTime = 500.millis
  val concurrency        = 5

  it should "not pull any items if not enough time to process" in {
    val waitTime = elementProcessTime / 2
    output[IO](initSt, 99, (x: Int) => IO.sleep(elementProcessTime).as(x)).compile.drain
      .timeout(waitTime)
      .attempt
      .unsafeRunSync()
      .swap
      .toOption
      .get shouldBe an[Exception]
  }

  it should "process messages concurrently according to state defined concurrency " in {
    val waitTimeOneBatch   = elementProcessTime * 1.5
    val waitTimeTwoBatches = elementProcessTime * 2.5

    val oneBatch = output[IO](
      initSt.copy(concurrency = concurrency),
      concurrency,
      (x: Int) => IO.sleep(elementProcessTime).as(x),
    )

    val twoBatches = output[IO](
      initSt.copy(concurrency = concurrency),
      concurrency * 2,
      (x: Int) => IO.sleep(elementProcessTime).as(x),
    )

    oneBatch.compile.to(Set).timeout(waitTimeOneBatch).unsafeRunSync() shouldBe (0 until concurrency).toSet
    twoBatches.compile.to(Set).timeout(waitTimeTwoBatches).unsafeRunSync() shouldBe (0 until concurrency * 2).toSet
  }
}
