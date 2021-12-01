package com.pingcap.lightning

import io.grpc.{CallOptions, Channel, ClientCall, ManagedChannel, MethodDescriptor}
import io.grpc.netty.NettyChannelBuilder
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import scala.collection.mutable

class SharedChannel(
  private var delegate: ManagedChannel,
  closeHook: () => Unit
) extends Channel {
  override def newCall[RequestT, ResponseT](
    methodDescriptor: MethodDescriptor[RequestT, ResponseT], callOptions: CallOptions):
  ClientCall[RequestT, ResponseT] = {
    delegate.newCall(methodDescriptor, callOptions)
  }

  override def authority(): String = {
    delegate.authority()
  }

  def close(): Unit = {
    if (delegate != null) {
      delegate = null
      closeHook()
    }
  }
}

class SharedChannelPool(idleTimeoutMillis: Long = TimeUnit.SECONDS.toMillis(60)) {
  private lazy val activeChannels = new mutable.HashMap[String, ReferenceCountedChannel]()
  private lazy val destroyer = Executors.newSingleThreadScheduledExecutor()

  private class ReferenceCountedChannel(val channel: ManagedChannel) {
    var refCnt = 1
    var destroyTask: ScheduledFuture[_] = _
  }

  def getChannel(addr: String): SharedChannel = synchronized {
    val channel = activeChannels.get(addr) match {
      case Some(rcChannel) =>
        if (rcChannel.destroyTask != null) {
          rcChannel.destroyTask.cancel(false)
          rcChannel.destroyTask = null
        }
        rcChannel.refCnt += 1
        rcChannel.channel
      case None =>
        val channel = NettyChannelBuilder.forTarget(addr).build()
        activeChannels.put(addr, new ReferenceCountedChannel(channel))
        channel
    }
    new SharedChannel(channel, () => {
      releaseChannel(addr)
    })
  }

  private def releaseChannel(addr: String): Unit = synchronized {
    val rcChannel = activeChannels(addr)
    rcChannel.refCnt -= 1
    assert(rcChannel.refCnt >= 0, "Reference count of channel shouldn't be negative")
    if (rcChannel.refCnt == 0) {
      rcChannel.destroyTask = destroyer.schedule(new Runnable {
        override def run(): Unit = {
          SharedChannelPool.this.synchronized {
            if (rcChannel.refCnt == 0) {
              try {
                rcChannel.channel.shutdown()
              } finally {
                activeChannels.remove(addr)
              }
            }
          }
        }
      }, idleTimeoutMillis, TimeUnit.MILLISECONDS)
    }
  }
}
