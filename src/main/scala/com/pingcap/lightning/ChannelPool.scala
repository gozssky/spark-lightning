package com.pingcap.lightning

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import scala.collection.mutable

object ChannelPool {
  private val idleTimeoutMillis = TimeUnit.SECONDS.toMillis(1)
  private lazy val destroyer = Executors.newSingleThreadScheduledExecutor()
  private lazy val instances = new mutable.HashMap[String, Instance]()

  private class Instance(val channel: ManagedChannel) {
    var refCnt = 1
    var destroyTask: ScheduledFuture[_] = _
  }

  def getChannel(addr: String): ManagedChannel = synchronized {
    instances.get(addr) match {
      case Some(instance) =>
        if (instance.destroyTask != null) {
          instance.destroyTask.cancel(false)
          instance.destroyTask = null
        }
        instance.refCnt += 1
        instance.channel
      case None =>
        val channel = NettyChannelBuilder.forTarget(addr).build()
        instances.put(addr, new Instance(channel))
        channel
    }
  }

  def releaseChannel(addr: String): Unit = synchronized {
    if (!instances.contains(addr)) {
      throw new IllegalArgumentException(s"No channel created for $addr")
    }
    val instance = instances(addr)
    if (instance.refCnt == 0) {
      throw new IllegalArgumentException("Number of release exceeds the number of get")
    }
    instance.refCnt -= 1
    if (instance.refCnt == 0) {
      instance.destroyTask = destroyer.schedule(new Runnable {
        override def run(): Unit = {
          ChannelPool.synchronized {
            if (instance.refCnt == 0) {
              try {
                instance.channel.shutdown()
              } finally {
                instances.remove(addr)
              }
            }
          }
        }
      }, idleTimeoutMillis, TimeUnit.MILLISECONDS)
    }
  }
}
