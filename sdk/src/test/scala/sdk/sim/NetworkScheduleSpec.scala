package sdk.sim

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import NetworkSchedule._

class NetworkScheduleSpec extends AnyFlatSpec with Matchers {
  "max throughput schedule" should "be correct" in {
    // define max throughput schedule
    val senders = Set(1, 2, 3)
    val ft      = NetworkSchedule.fullThrottleSchedule[Int, Int](senders, identity)
    // under full throttle schedule each sender creates a block observing exactly one message from all senders
    ft.schedule
      .take(10)
      .forall(_.iterator.map(_._2).forall(_.toObserve.count(_._2 == 1) == senders.size)) should be(true)
  }

  "isolation" should "work" in {
    val senders = Set(1, 2, 3)
    val ft      = NetworkSchedule.fullThrottleSchedule[Int, Int](senders, identity)
    // isolate senders 1 and 2 through steps 1 and 2
    val is      = NetworkSchedule.withIsolation(ft, NetworkSchedule.Isolation(0 to 2, (1, 2))).schedule.take(10).toList
    // through steps 1 and 2 there should not be message exchange between senders 1 and 2 so this should fail
    is.forall(x => x.values.forall(_.toObserve.count(_._2 == 1) == senders.size)) should be(false)
    // exchange structure should be as defined through steps 0, 1 and 2
    // 1 and 2 should receive 1 message from 3 plus previous self messages
    is.take(3).forall {
      _.collect { case s -> ToCreate(_, x) if s == 1 => x == senders.map(_ -> 1).toMap + (2 -> 0) }.forall(identity)
    } should be(true)
    is.take(3).forall {
      _.collect { case s -> ToCreate(_, x) if s == 2 => x == senders.map(_ -> 1).toMap + (1 -> 0) }.forall(identity)
    } should be(true)
    // 3 should receive one message from all
    is.take(3).forall {
      _.collect { case 3 -> ToCreate(_, x) => x == Map(1 -> 1, 2 -> 1, 3 -> 1) }.forall(identity)
    } should be(true)
    // after isolation lifted everyone should see everyone
    is.drop(3).forall {
      _.collect { case s -> ToCreate(_, x) if s == 1 => x == senders.map(_ -> 1).toMap }.forall(identity)
    } should be(true)
    is.drop(3).forall {
      _.collect { case s -> ToCreate(_, x) if s == 2 => x == senders.map(_ -> 1).toMap }.forall(identity)
    } should be(true)
  }

  "random schedule" should "work" in {
    val genMForS: Int => org.scalacheck.Gen[Int] = _ => org.scalacheck.Gen.chooseNum(0, 100)
    val genS: org.scalacheck.Gen[Int]            = org.scalacheck.Gen.chooseNum(0, 100)
    val rs                                       = NetworkScheduleGen.random(genMForS, genS, 100, 100).sample.get
    println(rs.schedule.take(10).toList)
  }
}
