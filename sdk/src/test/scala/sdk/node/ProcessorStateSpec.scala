package sdk.node

import cats.syntax.all.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.collection.immutable

class ProcessorStateSpec extends AnyFlatSpec {
  val p = Processor.default[Int]()

  it should "implement structural equivalence" in {
    val p1      = Processor.default[Int]()
    val p2      = Processor.default[Int]()
    val (p3, _) = p2.receive(0)
    p1 == p2 shouldBe true
    p1 == p3 shouldBe false
  }

  "init state" should "be empty" in {
    p.processingSet shouldBe Set()
  }

  it should "allow processing" in {
    val (_, r) = p.receive(1)
    r shouldBe true
  }

  it should "not allow starting another processing for the same item until it's done" in {
    val (p1, _) = p.receive(1)
    val (_, r)  = p1.receive(1)
    r shouldBe false
  }

  // Items that are done are supposed to be stored in some upstream state which should be responsible for
  // detecting duplicates if required
  it should "allow starting another processing for the same item after it's done" in {
    val (p1, _) = p.receive(1)
    val (_, r)  = p1.next._1.done(1).receive(1)
    r shouldBe true
  }

  it should "allow concurrent processing for different items" in {
    val (p1, r1) = p.receive(1)
    val (_, r2)  = p1.receive(2)
    r1 shouldBe true
    r2 shouldBe true
  }

  "receive" should "put message in the end of waiting queue" in {
    val (p1, _) = p.receive(1)._1.receive(2)._1.receive(3)
    p1.waitingList shouldBe immutable.Vector(1, 2, 3)
  }

  "retry" should "put message in the beginning of waiting queue" in {
    val (p1, _) = p.retry(1)._1.retry(2)._1.retry(3)
    p1.waitingList shouldBe immutable.Vector(3, 2, 1)
  }

  "next" should "output an item if waiting list is not empty and concurrency limit is not reached" in {
    val (_, n) = p.receive(0)._1.next
    n shouldBe 0.some
  }

  "next" should "not output item if waiting list is not empty but concurrency limit is reached or vice versa" in {
    // waiting list is empty - nothing to process
    p.copy(concurrency = 5).next._2 shouldBe None

    // receive item and start processing
    val (p1, _)  = p.copy(concurrency = 1).receive(0)._1.next
    // the second item won't be output until the first one if finished
    val (p11, n) = p1.receive(1)._1.next
    n shouldBe None                    // nothing to process
    p11.waitingList shouldBe Vector(1) // though waiting list is not empty

    // item should be output after the first on is done
    val p2 = p11.done(0).next
    p2._2 shouldBe 1.some
    p2._1.waitingList shouldBe Vector()
  }
}
