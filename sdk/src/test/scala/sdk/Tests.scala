package sdk

import org.scalatest.flatspec.AnyFlatSpec
import cats.effect.{IO, Sync}
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import fs2.Stream
import sdk.syntax.all.{logTime, logTimeF}

import scala.concurrent.duration.DurationInt
import scala.util.Random

class Tests extends AnyFlatSpec {
  "test" should "pass" in {

    val a = Queue.synchronous[IO, Int].map { q =>

      val prod = Stream
        .emits(1 to 100)
        .covary[IO]
        .evalTap(i => println(s"$i before offer").pure[IO])
        .evalTap(q.offer)
        .evalTap(i => println(s"$i after offer").pure[IO])

      val take =
        Stream
          .fromQueueUnterminated(q)
          .parEvalMap(10)(x => IO.sleep(1.second).as(x))
          .evalTap(i => println(s"$i taken").pure[IO])

      take concurrently prod
    }

    a.unsafeRunSync().take(100).compile.drain.unsafeRunSync()
  }

  "infinite next" should "" in {
    val rnd = new Random()
    final case class ST() {
      def next = {
        val rand = rnd.between(0, 100)
        if (rand < 50) none[Int] else rand.some
      }
    }
  }

  "time" should "" in {
    lazy val a = Sync[IO].delay("23344").logTimeF
    a.replicateA(100).unsafeRunSync()
  }
}
