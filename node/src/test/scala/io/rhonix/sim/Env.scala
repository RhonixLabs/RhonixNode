package io.rhonix.sim

import cats.FlatMap
import cats.effect.kernel.Temporal
import cats.effect.std.Random
import cats.syntax.all.*
import dproc.DProc
import dproc.data.Block
import fs2.Pipe
import io.rhonix.sim.Sim.*
import weaver.{Lazo, Meld, Weaver}
import weaver.data.LazoE

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration

/** Mocks for real world things. */
object Env {

  /** Shared block store across simulation. */
  val blocks = TrieMap.empty[M, Block.WithId[M, S, T]]

  def randomTGen[F[_]: Random] = Random[F].nextString(10)

//  def transactionStream[F[_]: Temporal: Random]: Stream[F, String] =
//    Stream.awakeEvery[F](2.seconds).evalMap { _ =>
//      val delayF =
//        Random[F].nextIntBounded(100).map(DurationInt(_).milliseconds).flatTap(Temporal[F].sleep)
//      delayF.flatMap(_ => randomTGen)
//    }e

  def broadcast[F[_]: Temporal](
    peers: List[(String, DProc[F, M, T])],
    time: Duration,
  ): Pipe[F, M, Unit] = _.evalMap { x =>
    Temporal[F].sleep(time) >> peers.traverse { case (peer, dProc) =>
//      println(s"$peer <- $x").pure
      dProc.acceptMsg(x)
    }.void
  }

//  def waitUntil[F[_]: Monad: Timer](condition: F[Boolean]): F[Unit] =
//    ().pure[F].tailRecM { _ =>
//      condition.map { r =>
//        if (r) ().asRight[F[Unit]] else Timer[F].sleep(50.milliseconds).asLeft[Unit]
//      }
//    }
//
//  def idealBCastAwaitingParents[F[_]: Concurrent: Timer](
//      peers: List[(String, DProc[F, M, S, T])]
//  ): Pipe[F, Msg.WithId[M, S, T], Unit] =
//    _.evalTap(_ => Timer[F].sleep(100.milliseconds)).through(idealBroadcast(peers))

//  def deployToRandom[F[_]: FlatMap: Random, M](sinks: List[M => F[Unit]]): Pipe[F, M, Unit] =
//    _.evalMap(m => Random[F].nextIntBounded(sinks.size).flatMap(r => sinks(r)(m)))

  // ids for messages of particular sender (no equivocation)
  // instead of using cryptographic hashing function
  def dummyIds(sender: S): LazyList[String] =
    LazyList.unfold(Map.empty[S, Int]) { acc =>
      val next   = acc.getOrElse(sender, 0) + 1
      val newAcc = acc + (sender -> next)
      val id     = s"$sender|-$next"
      (id, newAcc).some
    }

  // Execution engine that does not change anything related to Lazo protocol,
  // unable to detect conflicts, dependencies and invalid execution
  def dummyExeEngine[F[_]: Temporal](genesisState: LazoE[S], exeTime: Duration) =
    Weaver.ExeEngine[F, M, S, T](
      new Lazo.ExeEngine[F, M, S] {
        override def replay(m: M): F[Boolean]               = Temporal[F].sleep(exeTime).as(true)
        override def finalData(fringe: Set[M]): F[LazoE[S]] = genesisState.pure
      },
      // TODO maintain map on tx generation to simulate conflicts and deps
      // no conflicts no dependencies
      new Meld.ExeEngine[F, T]    {
        override def conflicts(l: T, r: T): F[Boolean] = false.pure
        override def depends(x: T, on: T): F[Boolean]  = false.pure
      },
    )
}
