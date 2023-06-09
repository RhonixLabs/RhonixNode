package dproc

import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all.*
import dproc.data.Block
import fs2.concurrent.Channel
import fs2.{Pipe, Stream}
import sdk.DagCausalQueue
import weaver.Weaver.ExeEngine
import weaver.*
import weaver.data.*

/**
  * Instance of an observer process.
  *
  * @param stateRef Ref holding the process state
  * @param gcStream stream of message that are garbage collected
  * @param finStream stream of finalized transactions
  * @param acceptMsg callback to make the process accept the block
  * @tparam F effect type
  */
final case class DProc[F[_], M, S, T](
  stateRef: Ref[F, Weaver[M, S, T]],           // state of the process - all data supporting the protocol
  ppStateRef: Option[Ref[F, sdk.Proposer]],    // state of the proposer
  pcStateRef: Ref[F, Processor.ST[M]],         // state of the message processor
  dProcStream: Stream[F, Unit],                // main stream that launches the process
  output: Stream[F, Block.WithId[M, S, T]],    // stream of output messages
  gcStream: Stream[F, Set[M]],                 // stream of messages garbage collected
  finStream: Stream[F, ConflictResolution[T]], // stream of finalized transactions
  acceptMsg: Block.WithId[M, S, T] => F[Unit], // callback to make process accept received block
  acceptTx: T => F[Unit],                      // callback to make the process accept transaction
)

object DProc {

  final case class WithId[F[_], Id, M, S, T](id: Id, p: DProc[F, M, S, T])

  final private case class StateWithTxs[M, S, T](state: Weaver[M, S, T], txs: Iterable[T])

  def connect[F[_]: Async, M, S, T](
    dProc: DProc[F, M, S, T],
    broadcast: Pipe[F, Block.WithId[M, S, T], Unit],
    finalized: Pipe[F, ConflictResolution[T], Unit],
    pruned: Pipe[F, Set[M], Unit],
  ): Stream[F, Unit] =
    dProc.dProcStream
      .concurrently(dProc.output.through(broadcast))
      .concurrently(dProc.finStream.through(finalized))
      .concurrently(dProc.gcStream.through(pruned))

  @SuppressWarnings(Array("org.wartremover.warts.ListAppend"))
  def apply[F[_]: Async, M, S, T: Ordering](
    idOpt: Option[S],
    initPool: List[T],
    initState: Weaver[M, S, T],
    exeEngine: ExeEngine[F, M, S, T],
    idGen: Block[M, S, T] => F[M],
    // DProc assumes that blocks are received in full and stored. Retrieval is out of scope
    getBlock: M => Block.WithId[M, S, T],
  ): F[DProc[F, M, S, T]] =
    for {
      // channel for incoming blocks, some load balancing can be applied by modifying this queue
      bQ          <- Channel.unbounded[F, Block.WithId[M, S, T]]
      // channel for incoming user signed transactions
      tQ          <- Channel.unbounded[F, T]
      // channel for outbound messages
      oQ          <- Channel.unbounded[F, Block.WithId[M, S, T]]
      // channel for garbage collect
      gcQ         <- Channel.unbounded[F, Set[M]]
      // channel for finalization results
      fQ          <- Channel.unbounded[F, ConflictResolution[T]]
      // states
      stRef       <- Ref[F].of(initState)
      plRef       <- Ref[F].of(initPool)
      proposerRef <- Ref[F].of(sdk.Proposer.default)
      // message processor
      concurrency  = 1000 // TODO CONFIG
      processor   <- Processor(stRef, exeEngine, concurrency)
      bufferStRef <- Ref[F].of(DagCausalQueue.default[M])
    } yield {
      val bufferAdd      = (m: M, d: Set[M]) =>
        bufferStRef.modify(_.enqueue(m, d).dequeue).flatMap(_.toList.traverse(x => processor.accept(getBlock(x))))
      val bufferComplete = (x: M) =>
        bufferStRef
          .modify { s =>
            val (newS, success) = s.satisfy(x)
            if (success) s.dequeue else newS -> Set()
          }
          .flatMap(_.toList.traverse(x => processor.accept(getBlock(x))))

      def proposeF(s: StateWithTxs[M, S, T]): F[Unit] = idOpt
        .map { sender =>
          for {
            // make message
            m <- MessageLogic.createMessage[F, M, S, T](s.txs.toList, sender, s.state, exeEngine)
            // assign ID
            b <- idGen(m).map(id => Block.WithId(id, m))
            // send to processing queue
            _ <- bQ.send(b)
          } yield ()
        }
        .getOrElse(Async[F].unit)

      // steam of incoming messages
      val receive = bQ.stream.evalMap(b => bufferAdd(b.id, b.m.minGenJs ++ b.m.offences))
      // stream of incoming tx
      val tS      = tQ.stream.evalTap(t => plRef.update(t +: _))
      val process = {
        // stream of states after new message processed.
        val afterAdded   = processor.results.evalMap { case (id, r) =>
          // send garbage and new finality detected (if any), notify buffer
          gcQ.send(r.garbage) >> r.finality.traverse(fQ.send) >> bufferComplete(id) >>
            // load transactions from pool and tuple the resulting state with it
            plRef.get.map(r.newState -> _)
        }
        // stream of states after new transaction received
        val afterNewTx   = tS
          .fold(initPool) { case (pool, tx) => pool :+ tx }
          .evalMap(newPool => stRef.get.map(_ -> newPool))
        // stream of states that the process should act on
        val statesStream = afterAdded.merge(afterNewTx).map { case (st, txs) => StateWithTxs(st, txs) }

        // make an attempt to propose after each message added
        statesStream.evalTap(sWt => proposerRef.modify(_.start).map(proposeF(sWt).whenA(_)))
      }

      val stream = process.void.concurrently(receive)

      new DProc[F, M, S, T](
        stRef,
        idOpt.map(_ => proposerRef),
        processor.stRef,
        stream,
        oQ.stream,
        gcQ.stream,
        fQ.stream,
        bQ.send(_).void,
        tQ.send(_).void,
      )
    }
}
