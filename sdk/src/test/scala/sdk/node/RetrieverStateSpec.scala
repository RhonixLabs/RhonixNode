//package sdk.node
//
//import cats.syntax.all.*
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should.Matchers
//
//import scala.concurrent.duration.FiniteDuration
//
//class RetrieverStateSpec extends AnyFlatSpec with Matchers {
//  val p = Retriever.default[Int]
//
//  it should "implement structural equivalence" in {
//    val p1      = Retriever.default[Int]
//    val p2      = Retriever.default[Int]
//    val (p3, _) = p2.request(1, FiniteDuration(1, "second"))
//    p1 == p2 shouldBe true
//    p1 == p3 shouldBe false
//  }
//
//  "init state" should "be empty" in {
//    p.statuses shouldBe Map()
//  }
//
//  "request" should "add new item if not present" in {
//    val now     = FiniteDuration(1, "second")
//    val (p1, _) = p.request(0, now)
//    val (p2, r) = p1.request(0, now)
//    p1.statuses shouldBe Map(0 -> Retriever.Requested(now))
//    p1 shouldBe p2
//    r shouldBe false
//  }
//
//  "receiveStarted" should "set correct status iif item was in Requested" in {
//    val now      = FiniteDuration(1, "second")
//    val (p1, _)  = p.request(0, now)
//    val (p2, r)  = p1.receiveStarted(0)
//    p2.statuses shouldBe Map(0 -> Retriever.ReceiveStarted)
//    p2 shouldBe p2
//    r shouldBe now.some
//    val (p3, r1) = p1.receiveStarted(1)
//    p3 shouldBe p1
//    r1 shouldBe None
//  }
//
//  "receiveDone" should "remove item iif item was in ReceiveStarted" in {
//    val now      = FiniteDuration(1, "second")
//    val (p1, _)  = p.request(0, now)
//    val (p2, _)  = p1.receiveStarted(0)
//    val (p3, r)  = p2.receiveDone(0)
//    p3.statuses shouldBe Map()
//    p3 shouldBe p3
//    r shouldBe true
//    val (p4, r1) = p1.receiveDone(1)
//    p4 shouldBe p1
//    r1 shouldBe false
//  }
//
//}
