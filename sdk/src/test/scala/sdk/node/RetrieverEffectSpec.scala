//package sdk.node
//
//import cats.effect.{IO, Ref}
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should.Matchers
//import sdk.node.Retriever.{receiveCase, Requested, ST}
//
//class RetrieverEffectSpec extends AnyFlatSpec with Matchers {
//  behavior of "receiveCase"
//
//  val ok        = IO.unit
//  val cancelled = IO.canceled
//  val failed    = IO.raiseError[Unit](new Exception())
//
//  it should "remove item from the state if processing succeed" in {
//    for {
//      st  <- Ref.of[IO, ST[Int]](Retriever.default[Int])
//      now <- IO.realTime
//      _   <- st.modify(_.request(0, now))
//      _   <- receiveCase(st, (_: Int) => ok).run(0)
//      r   <- st.get.map(_.statuses.get(0))
//    } yield r shouldBe None
//  }
//
//  it should "put back request with the same timestamp if processing failed of cancelled" in {
//    for {
//      st  <- Ref.of[IO, ST[Int]](Retriever.default[Int])
//      now <- IO.realTime
//      _   <- st.modify(_.request(0, now))
//      _   <- receiveCase(st, (_: Int) => failed).run(0)
//      r1  <- st.get.map(_.statuses.get(0))
//      _   <- receiveCase(st, (_: Int) => cancelled).run(0)
//      r2  <- st.get.map(_.statuses.get(0))
//    } yield {
//      r1 shouldBe Requested(now)
//      r2 shouldBe Requested(now)
//    }
//  }
//
//  behavior of "retriever"
//
//  // TODO
//  it should "ignore receives for items that was not first requested" in {}
//
//  // TODO
//  "output stream" should "contain all items successfully received" in {}
//}
