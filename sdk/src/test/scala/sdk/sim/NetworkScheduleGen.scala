package sdk.sim

import cats.Applicative
import org.scalacheck.Gen
import cats.syntax.all.*

import scala.collection.concurrent.TrieMap

object NetworkScheduleGen {
  import NetworkSchedule._

  implicit val genApplicative: Applicative[Gen] = new Applicative[Gen] {
    override def pure[A](x: A): Gen[A]                         = Gen.const(x)
    override def ap[A, B](ff: Gen[A => B])(fa: Gen[A]): Gen[B] = ff.flatMap(f => fa.map(f))
  }

  def random[M, S](genMForS: S => Gen[M], genS: Gen[S], nBlk: Int, nVal: Int): Gen[NetworkSchedule[M, S]] =
    for {
      senders  <- Gen.listOfN(nVal, genS).map(_.toSet)
      // possible number of blocks to see from each sender. 0 means no blocks, 1 means one block, etc.
      // 0 twice to make it more frequent
      numSee    = Seq(0, 0, 1, 2, 3)
      toSee     = Gen.oneOf(numSee).map(nBlocks => senders.map(_ -> nBlocks).toMap)
      // Todo extract separately
      lvl       = Gen
                    .someOf(senders)
                    // random subset of senders that create blocks concurrently
                    .flatMap { concurrentSenders =>
                      concurrentSenders.toSeq.traverse { sId =>
                        // generate id for message
                        genMForS(sId).flatMap { mId =>
                          // .map(_ + (sId -> 1) here to ensure always a single block is observed from self
                          toSee.map(_ + (sId -> 1)).map(sId -> ToCreate(mId, _))
                        }
                      }
                    }
      genesis  <- Gen.oneOf(senders).flatMap(s => genMForS(s).map(m => s -> ToCreate[M, S](m, Map())))
      schedule <- Gen.infiniteLazyList(lvl.map(_.toMap)).map(_.take(nBlk))
    } yield NetworkSchedule(senders, schedule.prepended(Map(genesis)))

  def randomWithId(): Gen[NetworkSchedule[String, String]] = {
    val seqNums = TrieMap.empty[String, Int]
    val genM    = (s: String) => Gen.const(s + "-" + seqNums.updateWith(s)(_.map(_ + 1).orElse(Some(0))).get)
    for {
      nBlk <- Gen.chooseNum(10, 100)
      nVal <- Gen.chooseNum(5, 15)
      r    <- random[String, String](genM, Gen.identifier.map(_.take(6)), nBlk, nVal)
    } yield r
  }

  def randomWithId(nBlk: Int, nVal: Int): Gen[NetworkSchedule[String, String]] = {
    val seqNums = TrieMap.empty[String, Int]
    val genM    = (s: String) => Gen.delay(s + "-" + seqNums.updateWith(s)(_.map(_ + 1).orElse(Some(0))).get)
    random[String, String](genM, Gen.identifier.map(_.take(6)), nBlk, nVal)
  }
}
