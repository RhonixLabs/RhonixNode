package weaver

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import weaver.rules.Dag.computeFJS

import scala.annotation.tailrec
import scala.collection.immutable.{Map, Set}

class LazoStateSpec extends AnyFlatSpec with Matchers {

//  "latest fringe" should "be computed OK" in {
//    val mgjs    = Set(0, 1, 2, 3)
//    val fringes = mgjs.map(x => x -> (x + 10)).toMap
//    latestFringeIdxOpt(mgjs, fringes) shouldBe 13.some
//    latestFringeIdxOpt(Set.empty[Int], Map.empty[Int, Int]) shouldBe none[Int]
//  }

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
