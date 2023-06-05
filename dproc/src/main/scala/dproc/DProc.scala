package dproc

import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all._
import dproc.Proposer.StateWithTxs
import dproc.data.Block
import fs2.concurrent.Channel
import fs2.{Pipe, Stream}
import io.rhonix.sdk.AsyncCausalBuffer
import weaver.Weaver.ExeEngine
import weaver._
import weaver.data._

/**
  * Instance of an observer process.
  *
  * @param weaverRef Ref holding the process state
  * @param gcStream stream of message that are garbage collected
  * @param finStream stream of finalized transactions
  * @param acceptMsg callback to make the process accept the block
  * @tparam F effect type
  */
final case class DProc[F[_], M, S, T](
  weaverRef: Ref[F, Weaver[M, S, T]], // state of the process - all data supporting the protocol
  ppStateRef: Option[Ref[F, Proposer.ST[M, S, T]]], // state of the proposer
  pcStateRef: Ref[F, Processor.ST[M]], // state of the message processor
  dProcStream: Stream[F, Unit], // main stream that launches the process
  output: Stream[F, Block.WithId[M, S, T]], // stream of output messages (proposals)
  gcStream: Stream[F, Set[M]], // stream of messages garbage collected
  finStream: Stream[F, ConflictResolution[T]], // stream of finalized transactions
  acceptMsg: Block.WithId[M, S, T] => F[Unit], // callback to make process accept received block
  acceptTx: T => F[Unit] // callback to make the process accept transaction
)

object DProc {

  final case class WithId[F[_], Id, M, S, T](id: Id, p: DProc[F, M, S, T])

  def connect[F[_]: Async, M, S, T](
    dProc: DProc[F, M, S, T],
    broadcast: Pipe[F, Block.WithId[M, S, T], Unit],
    finalized: Pipe[F, ConflictResolution[T], Unit],
    pruned: Pipe[F, Set[M], Unit]
  ): Stream[F, Unit] =
    dProc.dProcStream
      .concurrently(dProc.output.through(broadcast))
      .concurrently(dProc.finStream.through(finalized))
      .concurrently(dProc.gcStream.through(pruned))

  def apply[F[_]: Async, M, S, T: Ordering](
    idOpt: Option[S],
    initPool: List[T],
    initState: Weaver[M, S, T],
    exeEngine: ExeEngine[F, M, S, T],
    idGen: Block[M, S, T] => F[M],
    // DProc assumes that blocks are received in full and stored. Retrieval is out of scope
    getBlock: M => Block.WithId[M, S, T]
  ): F[DProc[F, M, S, T]] =
    for {
      // channel for incoming blocks, some load balancing can be applied by modifying this queue
      bQ <- Channel.unbounded[F, Block.WithId[M, S, T]]
      // channel for incoming user signed transactions
      tQ <- Channel.unbounded[F, T]
      // channel for outbound messages
      oQ <- Channel.unbounded[F, Block.WithId[M, S, T]]
      // channel for garbage collect
      gcQ <- Channel.unbounded[F, Set[M]]
      // channel for finalization results
      fQ <- Channel.unbounded[F, ConflictResolution[T]]
      // states
      stRef <- Ref[F].of(initState)
      plRef <- Ref[F].of(initPool)
      // proposer (if self id is set)
      proposerOpt <- idOpt.traverse(Proposer(_, exeEngine, idGen))
      // message processor
      concurrency = 1000 // TODO CONFIG
      processor <- Processor(stRef, exeEngine, concurrency)
      // buffer for input blocks ensuring they are fed to processor in causal order
      buffer <- AsyncCausalBuffer[F, M]()
    } yield {
      // steam of incoming messages
      val mS = bQ.stream
      // stream of incoming tx
      val tS = tQ.stream.evalTap(t => plRef.update(t +: _))
      // data for causal buffer
      val jsF = (m: M) => getBlock(m).m.minGenJs
      val completedFilter = (x: Set[M]) => stRef.get.map(s => x.filter(s.lazo.contains))
      // message receiver
      val receive = mS.map(_.id).through(buffer.pipe(jsF, completedFilter)).map(getBlock).evalMap(processor.accept)
      val propose = proposerOpt.map(_.out.evalTap(bQ.send)).getOrElse(Stream.empty)
      val process = {
        // stream of states after new message processed.
        val afterAdded = processor.results.evalMap { case (id, r) =>
          // send garbage and new finality detected (if any), notify buffer
          gcQ.send(r.garbage) >> r.finality.traverse(fQ.send) >> buffer.complete(Set(id)) >>
            // load transactions from pool and tuple the resulting state with it
            plRef.get.map(r.newState -> _)
        }
        // stream of states after new transaction received
        val afterNewTx = tS
          .fold(initPool) { case (pool, tx) => pool :+ tx }
          .evalMap(newPool => stRef.get.map(_ -> newPool))
        // stream of states that the process should act on
        val statesStream = afterAdded.merge(afterNewTx).map { case (st, txs) => StateWithTxs(st, txs) }

        // make an attempt to propose after each message added
        statesStream.evalTap(sWt => proposerOpt.traverse(_.tryPropose(sWt)))
      }

      val stream = process.concurrently(receive).concurrently(propose)

      new DProc[F, M, S, T](
        stRef,
        proposerOpt.map(_.stRef),
        processor.stRef,
        stream.void,
        oQ.stream,
        gcQ.stream,
        fQ.stream,
        bQ.send(_).void,
        tQ.send(_).void
      )
    }
}
