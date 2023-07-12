package dproc

import cats.effect.Ref
import cats.effect.kernel.Async
import cats.effect.std.{Mutex, Queue}
import cats.syntax.all.*
import dproc.WeaverNode.*
import dproc.data.Block
import fs2.Stream
import sdk.DagCausalQueue
import sdk.node.{Processor, Proposer}
import weaver.Weaver
import weaver.Weaver.ExeEngine
import weaver.data.ConflictResolution
import sdk.syntax.all.*

/**
  * Process of an distributed computer.
  * @tparam F effect type
  */
final case class DProc[F[_], M, T](
  dProcStream: Stream[F, Unit],                // main stream that launches the process
  // state
  ppStateRef: Option[Ref[F, Proposer.ST]],     // state of the proposer
  pcStateRef: Ref[F, Processor.ST[M]],         // state of the processor
  // outputs
  output: Stream[F, M],                        // stream of messages added to the state
  gcStream: Stream[F, Set[M]],                 // stream of messages garbage collected
  finStream: Stream[F, ConflictResolution[T]], // stream of finalized transactions
  // inputs transactions and blocks has to be stored before sending to dProc, these callbacks accept only ids
  acceptMsg: M => F[Unit],                     // callback to make process accept received block
  acceptTx: T => F[Unit],                      // callback to make the process accept transaction
)

object DProc {

  def apply[F[_]: Async, M, S, T: Ordering](
    // states
    weaverStRef: Ref[F, Weaver[M, S, T]],    // weaver
    proposerStRef: Ref[F, Proposer.ST],      // proposer
    processorStRef: Ref[F, Processor.ST[M]], // processor
    readTxs: => F[Set[T]],                   // tx pool
    bufferStRef: Ref[F, DagCausalQueue[M]],  // buffer
    // id of the process if node is supposed to propose
    idOpt: Option[S],
    // execution engine
    exeEngine: ExeEngine[F, M, S, T],
    // id generator for a block created
    idGen: Block[M, S, T] => F[M],
    // save block to persistent storage
    saveBlock: Block.WithId[M, S, T] => F[Unit],
    loadBlock: M => F[Block.WithId[M, S, T]],
  ): F[DProc[F, M, T]] =
    for {
      // channel for incoming transactions
      txQ <- Queue.unbounded[F, T]
      // channel for inbound messages
      inQ <- Queue.unbounded[F, M]
      // channel for outbound messages
      oQ  <- Queue.unbounded[F, M]
      // channel for garbage collect
      gcQ <- Queue.unbounded[F, Set[M]]
      // channel for finalization results
      fQ  <- Queue.unbounded[F, ConflictResolution[T]]
      x   <- procWithTrig(
               weaverStRef,
               exeEngine,
               processorStRef,
               loadBlock,
             )
      y   <- propWithTrig(
               idOpt,
               proposerStRef,
               weaverStRef,
               readTxs,
               exeEngine,
               idGen,
             )

      (addedStream, triggerProcessing)  = x
      (proposeStream, triggerProposing) = y
      // lock to protect read of missing dependencies until they are recorded to buffer
      misDepReadMutex                  <- Mutex[F]
    } yield {

      val effects = new NodeFlow.Effects[F, M, S, T, AddEffect[M, T], Block.WithId[M, S, T]] {
        override def bufferIncoming(m: M): F[Unit] = for {
          b    <- loadBlock(m)
          next <- misDepReadMutex.lock.use(_ =>
                    weaverStRef.get
                      .map(w => (b.m.minGenJs ++ b.m.offences).filterNot(w.lazo.contains))
                      .flatMap(missing => bufferStRef.modify(_.enqueue(b.id, missing).dequeue)),
                  )
//          // this is required in case between computation of missing dependencies
//          // and call on a buffer state another update happen that moves some dependency moved from
//          // buffer to weaver.
//          toSatisfy <- weaverStRef.get.map(w => missing.filter(w.lazo.contains))
//          next      <- bufferStRef.modify(s => toSatisfy.foldLeft(s) { case (acc, i) => acc.satisfy(i)._1 }.dequeue)
          _    <- next.toList.traverse(triggerProcessing)
        } yield ()

        override def added(m: M, r: AddEffect[M, T]): F[Unit] = for {
          selfBlock <- idOpt.traverse(id => loadBlock(m).map(_.m.sender).map(_ == id)).map(_.getOrElse(false))
          _         <- new Exception(s"Validation of a self block failed ${r.offenceOpt}").raiseError
                         .whenA(r.offenceOpt.nonEmpty && selfBlock)
          // notify processor
          _         <- processorStRef.update(_.done(m))

          // notify proposer
          selfBlock <- idOpt.traverse(self => loadBlock(m).map(_.m.sender == self)).map(_.getOrElse(false))
          _         <- proposerStRef.modify(_.done).whenA(selfBlock)

          // notify buffer, get next
          next <- misDepReadMutex.lock.use(_ => bufferStRef.modify(_.satisfy(m)._1.dequeue))
          _    <- next.toList.traverse_(triggerProcessing)

          // send output
          _ <- oQ.offer(m)
          _ <- gcQ.offer(r.garbage)
          _ <- r.finalityOpt.traverse_(fQ.offer)
        } yield ()

        override def store(b: Block.WithId[M, S, T]): F[Unit] = saveBlock(b)

        override def triggerProcess(m: M): F[Unit] = triggerProcessing(m)

        override def triggerPropose: F[Unit] = triggerProposing
      }

      val input    = Stream.fromQueueUnterminated(inQ)
      val txStream = Stream.fromQueueUnterminated(txQ)

      val s = dproc.NodeFlow[F, M, S, T, AddEffect[M, T], Block.WithId[M, S, T]](
        input,
        addedStream,
        txStream,
        proposeStream,
        effects,
        _.id,
      )

      new DProc[F, M, T](
        s,
        idOpt.map(_ => proposerStRef),
        processorStRef,
        Stream.fromQueueUnterminated(oQ),
        Stream.fromQueueUnterminated(gcQ),
        Stream.fromQueueUnterminated(fQ),
        inQ.offer,
        txQ.offer,
      )
    }
}
