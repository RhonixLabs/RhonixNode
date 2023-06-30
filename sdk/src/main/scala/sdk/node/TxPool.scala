package sdk.node

object TxPool {
  final case class ST[T](txs: Set[T]) {
    def next: ST[T]                     = ST(txs.tail)
    def finalize(finals: Set[T]): ST[T] = ST(txs -- finals)
  }
}
