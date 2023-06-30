package sdk.node

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import sdk.node.Proposer.*

import scala.util.Random

class ProposerStateSpec extends AnyFlatSpec {
  val p = Proposer.default

  it should "init status should be Idle" in {
    p.status shouldBe Idle
  }

  it should "implement structural equivalence" in {
    val p1 = Proposer.default
    val p2 = Proposer.default
    // two instances should be the equal
    p1 == p2 shouldBe true
    // transition the second instance into started state, now not equal
    p1 == p2.schedule shouldBe false
  }

  it should "successfully trigger propose when idle and scheduled" in {
    // start propose
    val (p1, r) = p.schedule.start
    // now status is Creating, return value is true
    p1.status shouldBe Creating
    r shouldBe true
  }

  it should "not trigger propose when idle but not scheduled" in {
    p.schedule.status shouldBe Idle
  }

  it should "not allow another propose until the first one is done" in {
    // generator for random actions which are not doneF
    val randomNotDone = LazyList.unfold(Set((x: ST) => x.start._1, (x: ST) => x.created)) { s =>
      val f = Random.shuffle(s).head
      Some(f, s)
    }

    // start propose
    val (p1, _) = p.schedule.start
    // notify that block is created
    val p2      = p1.created
    p2.status shouldBe Adding // now block is being added
    // call 20 random events which are not Done
    val p3 = randomNotDone.take(20).foldLeft(p2) { case (acc, f) => f(acc) }
    // proposer should still tell that block is being added
    p3.status shouldBe Adding
    // state should remain unchanged
    p2 shouldBe p3
  }

  "done after start without first calling complete" should "not change the status" in {
    // start propose
    val (p1, _) = p.schedule.start
    p1.status shouldBe Creating
    // call done - should be still creating
    p1.done._1.status shouldBe Creating
  }

  it should "make new propose when created and done" in {
    // call start, created, done
    val p2 = p.schedule.start._1.created.done
    // another propose is successful
    p2._1.schedule.start._2 shouldBe true
  }
}
