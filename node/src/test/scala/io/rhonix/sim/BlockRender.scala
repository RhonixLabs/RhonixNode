package io.rhonix.sim

import cats.Show
import cats.effect.kernel.Ref.Make
import cats.effect.{Async, Ref}
import cats.syntax.all.*
import sdk.DagCausalQueue
import sdk.graphviz.GraphGenerator.*
import sdk.graphviz.{GraphSerializer, ListSerializerRef}
import weaver.Lazo
import weaver.syntax.all.*

import java.nio.file.Paths

object BlockRender {
  def renderLazoMessage[F[_]: Async: Make, M: Show, S: Show](lazo: Lazo[M, S], target: M): F[Unit] =
    for {
      // all blocks in the scope of target block
      messages     <- Async[F].delay {
                        lazo
                          .scope(target)
                          .map { x =>
                            val data = lazo.dagData(x)
                            x.show -> ValidatorBlock(
                              x.show,
                              data.sender.show,
                              0,
                              data.jss.map(_.show).toList,
                              lazo.fringes(data.fringeIdx).map(_.show),
                            )
                          }
                          .toMap
                      }
      // ids of messages defining the scope of target message
      scopeSet      = messages.keySet
      // dag is stored just in a Ref, validation is just height computation
      dagRef       <- Ref.of[F, Map[String, Int]](Map())
      // this is equal to validation process, here just block height is assigned
      computeHeight = (m: String) =>
                        dagRef.update { dag =>
                          val existingJs = messages(m).justifications.toSet.intersect(scopeSet)
                          val height     =
                            existingJs.nonEmpty
                              .guard[Option]
                              .fold(0)(_ => existingJs.map(dag).maxOption.map(_ + 1).getOrElse(0))
                          dag.updated(m, height)
                        }
      // send messages through buffer
      jsF           = (m: String) => messages(m).justifications.toSet.intersect(scopeSet)
      satisfiedF    = (m: Set[String]) => dagRef.get.map(v => m.filter(v.contains))
      // buffer instance
      buffer       <- Ref.of(DagCausalQueue.default[String])
      _            <- messages.keysIterator.toList.traverse(x => buffer.update(_.enqueue(x, jsF(x))))
      a            <- fs2.Stream
                        .repeatEval(buffer.modify(_.dequeue).flatTap(_.toList.traverse(x => buffer.modify(_.satisfy(x)))))
                        .takeWhile(_.nonEmpty)
                        .compile
                        .toList
      _            <- a.traverse(_.toList.traverse(computeHeight))
      // read validated
      blocks       <- dagRef.get.map { heights =>
                        messages.view.mapValues(x => x.copy(height = heights(x.id).toLong)).values.toVector
                      }

      // render the message scope
      ref          <- Ref[F].of(Vector[String]())
      _            <- {
        implicit val ser: GraphSerializer[F] = new ListSerializerRef[F](ref)
        dagAsCluster[F](blocks)
      }
      res          <- ref.get
      name          = "DAG"
      _             = {
        val graphString = res.mkString

        val filePrefix = s"vdags/$name"

        // Ensure directory exists
        val dir = Paths.get(filePrefix).getParent.toFile
        if (!dir.exists()) dir.mkdirs()

        // Save.graph(file)
        import java.nio.file.*
        Files.writeString(Path.of(s"$filePrefix.dot"), graphString)

        // Generate dot image
        import java.io.ByteArrayInputStream
        import scala.sys.process.*

        val imgType  = "jpg"
        val fileName = s"$filePrefix.$imgType"
        println(s"Generating dot image: $fileName")

        val dotCmd = Seq("dot", s"-T$imgType", "-o", fileName)

        val arg = new ByteArrayInputStream(graphString.getBytes)
        dotCmd.#<(arg).!
      }
    } yield ()
}
