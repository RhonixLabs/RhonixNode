package weaver

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import weaver.rules.Dag._

import scala.annotation.tailrec

class LazoDagSpec extends AnyFlatSpec with Matchers {

  "computeFullJS" should "be OK" in {
    val minGenJs         = Set(0, 1)
    val js               = Map(0 -> Set(2, 3, 4, 5), 1 -> Set(2, 3, 6, 7))
    val isSelfDescendant = (x: Int, y: Int) => x < y
    val bonded           = Set(10, 20, 30, 40)
    val sender           = Map(0 -> 10, 1 -> 20, 2 -> 10, 3 -> 20, 4 -> 30, 5 -> 40, 6 -> 30, 7 -> 40)
    val fullJs           = computeFJS(minGenJs, bonded, js, isSelfDescendant, sender)
    fullJs shouldBe Set(0, 1, 4, 5)
    val withoutSender10  = computeFJS(minGenJs, bonded - 30, js, isSelfDescendant, sender)
    withoutSender10 shouldBe Set(0, 1, 5)
  }

  "computeMinGenJsSet" should "be OK" in {
    val js      = Set(1, 2, 3, 4)
    val seenMap = Map(1 -> Set(2, 3), 2 -> Set(3))
    val mgjs    = computeMGJS(js, (x: Int, y: Int) => seenMap.get(x).exists(_.contains(y)))
    mgjs shouldBe Set(1, 4)
  }

  "floor" should "be OK" in {
    val fringes          = Set(Set(0, 1, 2), Set(0, 3, 4))
    val isSelfDescendant = (x: Int, y: Int) => x > y
    val sender           = Map(0 -> 10, 1 -> 11, 2 -> 12, 3 -> 11, 4 -> 12)
    val x                = floor(fringes, isSelfDescendant, sender)
    x shouldBe Set(0, 1, 2)
  }

  "seenByAll" should "be OK" in {
    val seenMap = Map(10 -> Set(1, 2), 11 -> Set(2, 3))
    seenByAll(Set(10, 11), seenMap) shouldBe Set(2)
  }

  // Message cannot be added if offender is in the view.

  // Message cannot be added if sender is not bonded in the view.

  // Justifications should be derived correctly from parents
  /** Ejections */
  // New ejection data should contain partition computed at the head position

  // New ejection data when exceeding size of ejection threshold should drop the latest item.

  /** Finality */
  // Next fringe target should find the highest messages for each sender
  // Partition should not be found when justification levels observe different justifications from senders outSe the partition

  /** Tests for fringe ceiling, floor, etc */
  /** Tests for transaction expiration. */
//  "Full justifications" should "be computed correctly" in {}
//  "Full justifications" should "be computed correctly in when bonding happens" in {
//    val target = Set(0)
//    val jssF   = Map(0 -> Map.empty[Int, Int])
//    val isAncestor = {
//      val ancestorMap = Map.empty[Int, Set[Int]]
//      (x: Int, y: Int) => ancestorMap.get(x).exists(_.contains(y))
//    }
//    val bonded  = Set(1, 2, 3, 4, 5) // 5 senders bonded
//    val senderF = Map(0 -> 1) // genesis created by sender 1
//    val jss     = computeFullJS(target, jssF, isAncestor, bonded, senderF)
//    // latest message is the same for each bonded sender ()
//    val ref = Map(5 -> 0, 1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0)
//    jss shouldBe ref
//  }
  def seen[M](m: M, jss: M => Set[M]) = {
    @tailrec
    def go(acc: Set[M], js: Set[M]): Set[M] =
      if (js.isEmpty) acc
      else {
        val nextJss = js.flatMap(jss)
        go(acc ++ nextJss, nextJss)
      }
    go(Set.empty[M], jss(m))
  }

  "Full justifications" should "be computed correctly in when bonding happens" in {
    val target      = Set(2)
    val jss         = Map(2 -> Set(1), 1 -> Set(0), 0 -> Set.empty[Int])
    val ancestorMap = jss.keys.map(k => k -> seen[Int](k, jss)).toMap
    val isAncestor  = (x: Int, y: Int) => ancestorMap.get(x).exists(_.contains(y))
    val bonded      = Set(1, 2, 3, 4, 5) // 5 senders bonded
    val senderF     = Map(0 -> 1)        // genesis created by sender 1
    val computed    = computeFJS(target, bonded, jss, isAncestor, senderF)
    // latest message is the same for each bonded sender ()
    val ref         = Map(5 -> 0, 1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0)
    computed shouldBe ref.values
  }
}
