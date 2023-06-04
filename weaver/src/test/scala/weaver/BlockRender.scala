package weaver

import cats.Show
import cats.effect.kernel.Ref.Make
import cats.effect.{Async, Ref}
import cats.syntax.all._
import io.rhonix.graphviz.GraphGenerator.{dagAsCluster, ValidatorBlock}
import io.rhonix.graphviz.{GraphSerializer, ListSerializerRef}
import io.rhonix.sdk.{AsyncCausalBuffer, DoublyLinkedDag}
import weaver.syntax.all._

import java.nio.file.Paths
import scala.concurrent.duration.DurationInt

object BlockRender {
  def renderLazoMessage[F[_]: Async: Make, M: Show, S: Show](lazo: Lazo[M, S], target: M): F[Unit] =
    for {
      // all blocks in the scope of target block
      messages <- Async[F].delay {
        lazo
          .scope(target)
          .map { x =>
            val data = lazo.dagData(x)
            x.show -> ValidatorBlock(
              x.show,
              data.sender.show,
              0,
              data.jss.map(_.show).toList,
              lazo.fringes(data.fringeIdx).map(_.show)
            )
          }
          .toMap
      }
      // ids of messages defining the scope of target message
      scopeSet = messages.keySet
      // dag is stored just in a Ref, validation is just height computation
      dagRef <- Ref.of[F, Map[String, Int]](Map())
      // buffer instance
      bufRef <- Ref.of[F, DoublyLinkedDag[String]](DoublyLinkedDag.empty[String])
      buffer <- AsyncCausalBuffer[F, String](bufRef)
      // this is equal to validation process, here just block height is assigned
      computeHeight = (m: String) =>
        dagRef.update { dag =>
          val existingJs = messages(m).justifications.toSet.intersect(scopeSet)
          val height =
            existingJs.nonEmpty.guard[Option].fold(0)(_ => existingJs.map(dag).maxOption.map(_ + 1).getOrElse(0))
          dag.updated(m, height)
        }
      // send messages through buffer
      jsF = (m: String) => messages(m).justifications.toSet.intersect(scopeSet)
      satisfiedF = (m: Set[String]) => dagRef.get.map { v => m.filter(v.contains) }
      _ <- fs2.Stream
        .fromIterator[F](messages.keysIterator, 1)
        .through(buffer.pipe(jsF, satisfiedF))
        // compute height, notify buffer
        .parEvalMap(1000)(m => computeHeight(m) >> buffer.complete(Set(m)))
        .interruptAfter(5.seconds)
        .compile
        .lastOrError
      // read validated
      blocks <- dagRef.get.map { heights =>
        messages.view.mapValues(x => x.copy(height = heights(x.id))).values.toVector
      }

      // render the message scope
      ref <- Ref[F].of(Vector[String]())
      _ <- {
        implicit val ser: GraphSerializer[F] = new ListSerializerRef[F](ref)
        dagAsCluster[F](blocks)
      }
      res <- ref.get
      name = "DAG"
      _ = {
        val graphString = res.mkString

        val filePrefix = s"vdags/$name"

        // Ensure directory exists
        val dir = Paths.get(filePrefix).getParent.toFile
        if (!dir.exists()) dir.mkdirs()

        // Save.graph(file)
        import java.nio.file._
        Files.writeString(Path.of(s"$filePrefix.dot"), graphString)

        // Generate dot image
        import java.io.ByteArrayInputStream
        import scala.sys.process._

        val imgType = "jpg"
        val fileName = s"$filePrefix.$imgType"
        println(s"Generating dot image: $fileName")

        val dotCmd = Seq("dot", s"-T$imgType", "-o", fileName)

        val arg = new ByteArrayInputStream(graphString.getBytes)
        dotCmd.#<(arg).!
      }
    } yield ()
}
