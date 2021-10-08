package com.pingcap.tikv

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.pingcap.kvproto.{PDGrpc, Pdpb}
import com.pingcap.tikv.PDClient._
import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.grpc.{ManagedChannel, StatusRuntimeException}
import io.grpc.netty.NettyChannelBuilder
import io.grpc.{Metadata, Status}
import io.grpc.Status.Code
import io.grpc.stub.MetadataUtils
import org.apache.log4j.{Level, LogManager}
import org.slf4j.LoggerFactory

import java.net.URL
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.util.Random


class PDClient(pdAddrOrURLs: Array[String], enableForwarding: Boolean = true) extends AutoCloseable {
  private val log = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  require(pdAddrOrURLs.nonEmpty && !pdAddrOrURLs.exists(_.isEmpty),
    "The length of pdAddrs must be greater than 0 and any pdAddr must not be empty.")

  @volatile private var pdAddrs = pdAddrOrURLs.map(PDClient.normalizeAddr)

  @volatile private var clusterID = 0L
  @volatile private var leaderAddr = ""
  @volatile private var followerAddrs = new Array[String](0)
  @volatile private var lastCheckLeaderElapsed = 0
  @volatile private var lastUpdateClusterElapsed = 0
  @volatile private var checkLeaderNow = false
  @volatile private var leaderNetworkFailure = false
  @volatile private var forwardingMetadata = new Metadata()
  private val channelPool = new ConcurrentHashMap[String, ManagedChannel]()

  private def initCluster(): Unit = {
    for (i <- 0 until MAX_INIT_CLUSTER_RETRY) {
      if (i > 0) {
        TimeUnit.MILLISECONDS.sleep(NEXT_INIT_CLUSTER_DELAY_MS)
      }
      if (updateCluster()) {
        return
      }
    }
    throw new RuntimeException("Failed to initialize cluster with all the given PD addresses")
  }

  initCluster()

  private val threadPool =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setDaemon(true).build())
  threadPool.scheduleWithFixedDelay(() => {
    lastCheckLeaderElapsed += 1
    lastUpdateClusterElapsed += 1
    if (lastCheckLeaderElapsed >= CHECK_LEADER_TICKS || checkLeaderNow) {
      checkLeaderHealth()
      lastCheckLeaderElapsed = 0
      checkLeaderNow = false
    }
    if (lastUpdateClusterElapsed >= UPDATE_CLUSTER_TICKS) {
      if (!updateCluster()) {
        log.error("Failed to update cluster after trying all PD addresses")
      }
      lastUpdateClusterElapsed = 0
    }
  }, MS_PER_TICK, MS_PER_TICK, TimeUnit.MILLISECONDS)


  private def getOrCreateChannel(addr: String): ManagedChannel = {
    channelPool.computeIfAbsent(addr,
      NettyChannelBuilder.forTarget(_).usePlaintext().build()
    )
  }

  private def removeUsedChannel(inUseAddrs: Array[String]): Unit = {
    channelPool.keys().asScala.toList
      .filter(key => !inUseAddrs.exists(_.equals(key)))
      .foreach(channelPool.remove(_).shutdown())
  }

  private def getMembers(addr: String): Pdpb.GetMembersResponse = {
    val channel = getOrCreateChannel(addr)
    val stub = PDGrpc.newBlockingStub(channel)
      .withDeadlineAfter(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
    stub.getMembers(Pdpb.GetMembersRequest.newBuilder().build())
  }

  private def updateCluster(): Boolean = {
    for (addr <- pdAddrs) {
      try {
        val resp = getMembers(addr)
        var allIsWell = true
        if (resp.getHeader == null) {
          log.warn(s"Failed to retrieve cluster id in the response from $addr")
          allIsWell = false
        }
        if (clusterID > 0 && resp.getHeader.getClusterId != clusterID) {
          log.warn(s"Cluster ID is changed unexpectedly. Please check your cluster." +
            s" (old-cluster-id: $clusterID, new-cluster-id: ${resp.getHeader.getClusterId})")
        }
        clusterID = resp.getHeader.getClusterId
        if (resp.getLeader == null || resp.getLeader.getClientUrlsList.isEmpty) {
          log.warn(s"Failed to retrieve leader's url in the response from $addr")
          allIsWell = false
        }
        if (allIsWell) {
          var changed = false
          val newLeaderAddr = PDClient.normalizeAddr(resp.getLeader.getClientUrls(0))
          if (newLeaderAddr != leaderAddr) {
            leaderAddr = newLeaderAddr
            forwardingMetadata = {
              val metadata = new Metadata()
              metadata.put(PDClient.FORWARDING_METADATA_KEY, leaderAddr)
              metadata
            }
            changed = true
          }
          val newFollowerAddrs = resp.getMembersList.stream()
            .filter(_.getMemberId != resp.getLeader.getMemberId)
            .flatMap(_.getClientUrlsList.stream())
            .collect(Collectors.toList[String])
            .asScala.toArray
            .map(PDClient.normalizeAddr)
          if (!newFollowerAddrs.sameElements(followerAddrs)) {
            followerAddrs = newFollowerAddrs
            changed = true
          }
          if (changed) {
            log.info(s"Updated cluster (cluster-id: $clusterID ,leader: $leaderAddr, followers: ${followerAddrs.mkString("(", ", ", ")")})")
            pdAddrs = Array(leaderAddr) ++ followerAddrs
            removeUsedChannel(pdAddrs)
          }
          return true
        }
      } catch {
        case e: StatusRuntimeException => log.warn(s"Failed to get members from $addr", e)
      }
    }
    false
  }

  private def checkLeaderHealth(): Unit = {
    val channel = getOrCreateChannel(leaderAddr)
    val stub = HealthGrpc.newBlockingStub(channel)
      .withDeadlineAfter(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
    leaderNetworkFailure = try {
      val resp = stub.check(HealthCheckRequest.newBuilder().build())
      resp.getStatus != HealthCheckResponse.ServingStatus.SERVING
    } catch {
      case e: StatusRuntimeException => isNetworkErrStatus(e.getStatus)
    }
    if (leaderNetworkFailure) {
      log.warn(s"Leader address $leaderAddr is unhealthy")
    }
  }

  private def getStub: PDGrpc.PDBlockingStub = {
    if (enableForwarding && leaderNetworkFailure) {
      val stubAndAddr = selectOneFollower
      if (stubAndAddr != null) {
        log.debug(s"Use follower address ${stubAndAddr._2} to connect PD")
        return stubAndAddr._1.withInterceptors(
          MetadataUtils.newAttachHeadersInterceptor(forwardingMetadata))
          .withDeadlineAfter(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }
    }
    PDGrpc.newBlockingStub(getOrCreateChannel(leaderAddr))
      .withDeadlineAfter(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
  }

  private def selectOneFollower: (PDGrpc.PDBlockingStub, String) = {
    val addrs = Random.shuffle(followerAddrs.toList)
    for (addr <- addrs) {
      val channel = getOrCreateChannel(addr)
      try {
        val resp = HealthGrpc.newBlockingStub(channel)
          .withDeadlineAfter(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
          .check(HealthCheckRequest.newBuilder().build())
        if (resp.getStatus == HealthCheckResponse.ServingStatus.SERVING) {
          return (PDGrpc.newBlockingStub(channel), addr)
        }
      } catch {
        case e: StatusRuntimeException => log.warn(s"Follower address $addr is unhealthy ", e)
      }
    }
    null
  }

  override def close(): Unit = {
    threadPool.shutdown()
    for (channel <- channelPool.values().asScala) {
      channel.shutdown()
    }
    channelPool.clear()
  }
}

private object PDClient {
  val MAX_INIT_CLUSTER_RETRY = 60
  val NEXT_INIT_CLUSTER_DELAY_MS = 1000
  val DEFAULT_TIMEOUT_MS = 10000 // 10s
  val MS_PER_TICK = 100
  val CHECK_LEADER_TICKS: Int = 1000 / MS_PER_TICK // 1s
  val UPDATE_CLUSTER_TICKS: Int = 10000 / MS_PER_TICK // 10s
  val FORWARDING_METADATA_KEY: Metadata.Key[String] =
    Metadata.Key.of("pd-forwarded-host", Metadata.ASCII_STRING_MARSHALLER)

  def normalizeAddr(addrOrURL: String): String = {
    val url = if (addrOrURL.contains("://")) {
      new URL(addrOrURL)
    } else {
      new URL(s"https://$addrOrURL")
    }
    if (url.getHost == null || url.getHost.isEmpty || url.getPort == 0) {
      throw new IllegalArgumentException(s"Address must be a valid url or a string in host:port format, but got $addrOrURL")
    }
    s"${url.getHost}:${url.getPort}"
  }

  def isNetworkErrStatus(status: Status): Boolean = {
    status.getCode == Code.UNAVAILABLE || status.getCode == Code.DEADLINE_EXCEEDED
  }
}

object PDClientTest {
  def main(args: Array[String]): Unit = {
    org.apache.log4j.BasicConfigurator.configure()
    LogManager.getRootLogger.setLevel(Level.INFO)

    val client = new PDClient(Array("127.0.0.1:2379", "127.0.0.1:2382", "127.0.0.1:2384"))
    client.close()
  }
}