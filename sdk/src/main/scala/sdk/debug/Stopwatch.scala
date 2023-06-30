package sdk.debug

import cats.effect.Sync
import cats.syntax.all.*

import scala.concurrent.duration.{Duration, FiniteDuration}

object Stopwatch {

  // TODO: this is temporary solution to measure and log duration.
  def time[F[_]: Sync, A](log: String => F[Unit])(tag: String)(block: => F[A]): F[A] =
    for {
      t0 <- Sync[F].delay(System.nanoTime)
      a  <- block
      t1  = System.nanoTime
      m   = Duration.fromNanos(t1 - t0)
      _  <- log(s"$tag [${showTime(m)}]")
    } yield a

  def durationRaw[F[_]: Sync, A](block: => F[A]): F[(A, FiniteDuration)] =
    for {
      t0 <- Sync[F].delay(System.nanoTime)
      a  <- block
      t1  = System.nanoTime
      m   = Duration.fromNanos(t1 - t0)
    } yield (a, m)

  def duration[F[_]: Sync, A](block: => F[A]): F[(A, String)] =
    durationRaw(block).map(_.map(showTime))

  def profile[A](block: => A): (A, String) = {
    val t0 = System.nanoTime
    val a  = block
    val t1 = System.nanoTime
    val m  = Duration.fromNanos(t1 - t0)
    (a, showTime(m))
  }

  def profileLog[A](block: => A): A = {
    val t0 = System.nanoTime
    val a  = block
    val t1 = System.nanoTime
    val m  = Duration.fromNanos(t1 - t0)
    println(showTime(m))
    a
  }

  def profileLogF[F[_]: Sync, A](block: => F[A]): F[A] = duration(block).map { case (r, t) =>
    println(t)
    r
  }

  def showTime(d: FiniteDuration): String = {
    val ns   = 1d
    val ms   = 1e6 * ns
    val sec  = 1000 * ms
    val min  = 60 * sec
    val hour = 60 * min
    val m    = d.toNanos
    if (m >= hour) s"${m / hour} hour"
    else if (m >= min) s"${m / min} min"
    else if (m >= sec) s"${m / sec} sec"
    else if (m >= ms) s"${m / ms} ms"
    else s"${m / 1e6d} ms"
  }

  trait DebugSyntax {
    final class Log[A](f: => A) {
      def logTime: A = profileLog(f)
    }

    final class LogF[F[_]: Sync, A](f: => F[A]) {
      def logTimeF: F[A] = profileLogF(f)
    }
    implicit def logTime[A](f: => A) = new Log[A](f)
    implicit def logTimeF[F[_]: Sync, A](f: => F[A]) = new LogF[F, A](f)
  }
}
