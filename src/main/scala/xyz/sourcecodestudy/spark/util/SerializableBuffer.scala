package xyz.sourcecodestudy.spark.util

import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.io.{ObjectInputStream, ObjectOutputStream, EOFException, IOException}


class SerializableBuffer(@transient var buffer: ByteBuffer) extends Serializable {

  def value: ByteBuffer = buffer

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    buffer = ByteBuffer.allocate(length)
    var amountRead = 0
    val channel = Channels.newChannel(in)
    while (amountRead < length) {
      val ret = channel.read(buffer)
      if (ret == -1) {
        throw new EOFException("End of file before fully reading buffer")
      }
      amountRead += ret
    }
    buffer.rewind() // Allow us to read it later
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.writeInt(buffer.limit())
    if (Channels.newChannel(out).write(buffer) != buffer.limit()) {
      throw new IOException("Could not fully write buffer to output stream")
    }
    buffer.rewind()
  }
}

object SerializableBuffer {
  def apply(buffer: ByteBuffer): SerializableBuffer = new SerializableBuffer(buffer)
}