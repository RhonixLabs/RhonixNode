package sdk.node

import cats.effect.kernel.Async
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.concurrent.SignallingRef
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sdk.node
import sdk.node.Proposer.*

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class ProposerEffectsSpec extends AnyFlatSpec with Matchers {

  behavior of "proposeBracket"

  "if proposal succeed" should "move state to Adding" in {
    for {
      stRef <- Ref.of[IO, ST](Proposer.default)
      _     <- proposeBracket(stRef, IO.unit)
      state <- stRef.get
    } yield state.status shouldBe node.Proposer.Adding
  }

  "if proposal is cancelled" should "revert state to Idle" in {
    for {
      stRef <- Ref.of[IO, ST](node.Proposer.default)
      // .start.flatMap(_.join) is a way to just get the outcome of a fiber,
      // since for cancelled fiber there is no return value
      _     <- proposeBracket(stRef, IO.canceled).start.flatMap(_.join)
      state <- stRef.get
    } yield state.status shouldBe node.Proposer.Idle
  }

  "if proposal is erred" should "revert state to Idle" in {
    for {
      stRef <- Ref.of[IO, ST](node.Proposer.default)
      _     <- proposeBracket(stRef, IO.raiseError(new Exception()))
      state <- stRef.get
    } yield state.status shouldBe node.Proposer.Idle
  }

  "if proposal succeed" should "return Right(value)" in {
    for {
      stRef <- Ref.of[IO, ST](node.Proposer.default)
      res   <- proposeBracket(stRef, IO.pure(42))
    } yield res shouldBe Right(42)
  }

  behavior of "proposer"

  it should "not emit equivocating proposals" in {
    def race[F[_]: Async](proposeDelay: FiniteDuration, ratio: Long, numAttempts: Long) = fs2.Stream.force(
      for {
        stRef               <- SignallingRef[F].of(node.Proposer.default)
        proposeF             = Async[F].sleep(proposeDelay)
        p                   <- Proposer[F, Unit](stRef, proposeF)
        (proposals, trigger) = p
        attempts             = fs2.Stream.repeatEval(trigger).metered(proposeDelay / ratio) take numAttempts
        // IMPORTANT - done has to be called after proposal is completed as the second step making sure
        // proposal has been not just created but also added added.
      } yield proposals.evalTap(_ => stRef.modify(_.done)) concurrently attempts,
    )

    val proposeDelay = 100.millis
    val ratio        = 10L
    val numAttempts  = 100L
    val r            = race[IO](proposeDelay, ratio, numAttempts)

    val numProposalsReference = (numAttempts / ratio) + 1
    import cats.effect.unsafe.implicits.global

    // Number of proposals should be correct
    // 100 millis is spent for one proposal creation
    // 10 proposal attempts for one propose, so attempts are going each other 10 millis
    // 100 attempts is 1 second, so this one should complete in about 1 second
    r.take(numProposalsReference)
      .compile
      .toList
      .timeout(2.seconds)
      .attempt
      .unsafeRunSync()
      .map(_.size) shouldBe Right(numProposalsReference)

    // No extra propose should be emitted, stream won't finish in 2 seconds
    r.take(numProposalsReference + 1)
      .compile
      .toList
      .timeout(2.seconds)
      .attempt
      .unsafeRunSync()
      .swap
      .toOption
      .get shouldBe an[Exception]
  }
}
