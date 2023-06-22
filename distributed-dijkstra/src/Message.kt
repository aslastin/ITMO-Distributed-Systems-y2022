package dijkstra.messages

sealed class Message

data class MessageWithData(val distance: Long) : Message()

object ChildMessage : Message()

object DoneChildMessage : Message()

object NonChildMessage : Message()
