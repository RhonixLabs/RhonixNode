package dproc

import cats.effect.Ref
import dproc.Discovery._

final case class Discovery[F[_], P](stRef: Ref[F, ST[P]])

object Discovery {
  final case class ST[P](peers: Set[P])
}
