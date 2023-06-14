package sdk

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import sdk.Proposer.*

import scala.util.Random

class ProposerSpec extends AnyFlatSpec {
  val p = Proposer.default

  it should "init status should be Idle" in {
    p.status shouldBe Idle
  }

  it should "successfully trigger propose when idle" in {
    // start propose
    val (p1, r) = p.start
    // now status is Creating, return value is true
    p1.status shouldBe Creating
    r shouldBe true
  }

  it should "not allow another propose until the first one is done" in {
    // generator for random actions which are not doneF
    val randomNotDone = LazyList.unfold(Set((x: Proposer) => x.start._1, (x: Proposer) => x.created)) { s =>
      val f = Random.shuffle(s).head
      Some(f, s)
    }

    // start propose
    val (p1, _) = p.start
    // notify that block is created
    val p2      = p1.created
    p2.status shouldBe Adding // now block is being added
    // call 20 random events which are not Done
    val p3 = randomNotDone.take(20).foldLeft(p2) { case (acc, f) => f(acc) }
    // proposer should still tell that block is being added
    p3.status shouldBe Adding
  }

  "done after start without first calling complete" should "not change the status" in {
    // start propose
    val (p1, _) = p.start
    p1.status shouldBe Creating
    // call done - should be still creating
    p1.done.status shouldBe Creating
  }

  it should "make new propose when created and done" in {
    // call start, created, done
    val p2 = p.start._1.created.done
    // another propose is successful
    p2.start._2 shouldBe true
  }
}