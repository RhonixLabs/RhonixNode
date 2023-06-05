package dproc

object Errors {

  object IncompleteMessageInProcessQueue extends Exception(s"Error. Message cannot be added but in process queue.")

}
