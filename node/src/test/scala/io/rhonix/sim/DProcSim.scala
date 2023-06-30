package io.rhonix.sim

import cats.effect.kernel.Async
import cats.effect.std.{Console, Random}
import cats.effect.{IO, IOApp, Ref}
import cats.syntax.all.*
import cats.{Applicative, Parallel}
import dproc.DProc
import dproc.data.Block
import fs2.{Pipe, Stream}
import io.rhonix.sim.Env.*
import sdk.DagCausalQueue
import sdk.node.{Processor, Proposer}
import weaver.Weaver
import weaver.data.*

import scala.concurrent.duration.DurationInt

object Sim extends IOApp.Simple {
  // Dummy types for message id, sender id and transaction id
  type M = String
  type S = String
  type T = String

  // TODO make distribution for delays
  // time spent for block to be executed
  val exeDelay  = 0.milliseconds
  // time for message to reach peers
  val propDelay = 0.milliseconds
  // time to pull the whole block
  val rcvDelay  = 0.milliseconds
  // time to compute message hash
  val hashDelay = 0.milliseconds
  // number of blocks to be produced by each sender
  val numBlocks = 50000

  /** Make instance of a process - peer or the network.
    * Init with last finalized state (lfs as the simplest). */
  def mkPeer[F[_]: Async: Random](
    id: S,
    lfs: Weaver[M, S, T],
    lfsExe: LazoE[S],
  ): F[DProc[F, M, S]] = for {
    weaverStRef    <- Ref.of(lfs)                       // weaver
    proposerStRef  <- Ref.of(Proposer.default)          // proposer
    processorStRef <- Ref.of(Processor.default[M](8))   // processor
    bufferStRef    <- Ref.of(DagCausalQueue.default[M]) // buffer

    exe = dummyExeEngine(lfsExe, exeDelay)

    idsRef <- Ref.of(dummyIds(id).take(numBlocks).toList)
    hasher  = (_: Block[M, S, T]) =>
                Async[F].sleep(hashDelay) >> idsRef.modify {
                  case head :: tail => (tail, head)
                  case _            => sys.error("No ids left")
                }

    saveBlock = (b: Block.WithId[M, S, T]) => blocks.update(b.id, b).pure[F]
    loadBlock = (id: M) => blocks(id).pure[F]

//    readTxs = Set.empty[String].pure
    readTxs = Random[F].nextString(10).map(Set(_))
    dproc  <- DProc[F, M, S, T](
                weaverStRef,
                proposerStRef,
                processorStRef,
                readTxs,
                bufferStRef,
                id.some,
                exe,
                hasher,
                saveBlock,
                loadBlock,
              )
  } yield dproc

  /** Make the computer, init all peers with lfs. */
  def mkNet[F[_]: Async: Random](lfs: LazoM[M, S]): F[List[(S, DProc[F, M, T])]] =
    lfs.state.bonds.activeSet.toList.traverse { vId =>
      mkPeer(vId, Weaver.empty(lfs.state), lfs.state).map(vId -> _)
    }

  /** Init simulation. Return list of streams representing processes of the computer. */
  def sim[F[_]: Async: Parallel: Random: Console](size: Int): F[List[Stream[F, Unit]]] = {
    val senders      = Iterator.range(0, size).map(n => s"s#$n").toList
    // Create lfs message, it has no parents, sees no offences and final fringe is empty set
    val genesisBonds = Bonds(senders.map(_ -> 100L).toMap)
    val genesisExec  = LazoE(genesisBonds, 50, 10000)
    val lfs          = LazoM[M, S]("s#0", Set(), Set(), LazoF(Set()), genesisExec)
    val genesisM     = {
      val genesisTx  = List.empty[T]
      val genesisFin = ConflictResolution[T](genesisTx.toSet, Set()).some
      Block.WithId(
        s"0@${senders.head}",
        Block[M, S, T](
          senders.head,
          Set(),
          Set(),
          genesisTx,
          Set(),
          genesisFin,
          Set(),
          genesisExec.bonds,
          genesisExec.lazinessTolerance,
          genesisExec.expirationThreshold,
        ),
      )
    }

    blocks.addOne(genesisM.id -> genesisM)

    def connect(
      dProc: DProc[F, M, T],
      broadcast: Pipe[F, M, Unit],
      finalized: Pipe[F, ConflictResolution[T], Unit],
      pruned: Pipe[F, Set[M], Unit],
    ): Stream[F, Unit] =
      dProc.dProcStream
        .concurrently(dProc.output.through(broadcast))
        .concurrently(dProc.finStream.through(finalized))
        .concurrently(dProc.gcStream.through(pruned))

    mkNet(lfs).map(_.zipWithIndex).flatMap { net =>
      net.traverse { case (self, dProc) -> idx =>
        val bootstrap = dProc.acceptMsg(genesisM.id) >> Console[F].println("Bootstrap done")
        val notSelf   = net.collect { case (x @ (s, _)) -> _ if s != self => x }
        val stream    = connect(
          dProc,
          broadcast(notSelf, propDelay),
//          measurePerSec.andThenF(x => Stream.eval(println(s"blocks per sec: $x").pure)),
          if (idx == 0) measurePerSec.andThenF(x => Stream.eval(println(s"tx per sec: $x").pure))
          else _ => Stream.empty,
//          _ => Stream.empty,
          identity[Stream[F, Set[M]]].map(_.void),
        )
        bootstrap.as(stream)
      }
    }
  }

  def TPS[F[_]: Async]: Pipe[F, ConflictResolution[T], Int] = { (in: Stream[F, ConflictResolution[T]]) =>
    in.flatMap(x => Stream.emits(x.accepted.toList)).through(measurePerSec)
  }

  def measurePerSec[F[_]: Async, I]: Pipe[F, I, Int] = {
    val acc = Ref.unsafe[F, Int](0)
    val out = Stream.eval(acc.getAndUpdate(_ => 0)).delayBy(1.second).repeat
    (in: Stream[F, I]) => out concurrently in.evalTap(_ => acc.update(_ + 1))
  }

  def crWithLog[F[_]: Applicative]: Pipe[F, ConflictResolution[T], ConflictResolution[T]] =
    (in: Stream[F, ConflictResolution[T]]) =>
      in.mapAccumulate(0) { case (acc, cr) => (acc + cr.accepted.size) -> cr }
        .evalTap(x => println(s"Finalized ${x._1} deploys").pure[F])
        .map(_._2)

  override def run: IO[Unit] =
    Random.scalaUtilRandom[IO].flatMap { implicit rndIO =>
      Sim.sim[IO](4).flatMap { processes =>
        Stream.emits(processes).parJoin(processes.size).compile.drain
      }
    }
}
