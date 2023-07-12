package sdk.sim

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Queue

class P2pSpec extends AnyFlatSpec with Matchers {
  val emptyP2P = P2P.empty[Int, Int](Set(0, 1, 2))

  "send" should "be correct" in {
    val sender = 0
    val msg = 0
    val p2p = emptyP2P.send(sender, msg)
    p2p shouldBe P2P(
      Map(0 -> Map(0 -> Queue(0), 1 -> Queue(), 2 -> Queue()),
          1 -> Map(0 -> Queue(0), 1 -> Queue(), 2 -> Queue()),
          2 -> Map(0 -> Queue(0), 1 -> Queue(), 2 -> Queue())
      )
    )
  }

  "observe" should "work" in {
    val sender = 0
    val msg = 0
    val (p2p, observed) = emptyP2P.send(sender, msg).observe(0, Map(0 -> 1))
    observed shouldBe Map(0 -> Some(0), 1 -> None, 2 -> None)
    p2p shouldBe P2P(
      Map(0 -> Map(0 -> Queue(), 1 -> Queue(), 2 -> Queue()),
          1 -> Map(0 -> Queue(0), 1 -> Queue(), 2 -> Queue()),
          2 -> Map(0 -> Queue(0), 1 -> Queue(), 2 -> Queue())
      )
    )
  }
}
