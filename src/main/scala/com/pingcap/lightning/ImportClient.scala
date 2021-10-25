package com.pingcap.lightning

import com.google.protobuf.ByteString
import com.pingcap.kvproto.ImportSstpb.SSTMeta
import com.pingcap.kvproto.{ImportSSTGrpc, ImportSstpb, Metapb}
import com.pingcap.lightning.ImportClient._
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver

import java.util.UUID
import java.util.concurrent.CountDownLatch
import scala.collection.JavaConverters._

class ImportClient(pdClient: PDClient, region: Region) {
  private val peers = region.meta.getPeersList.asScala
  private val storeAddrs = new Array[String](peers.size)
  private val writeClients = new Array[WriteClient](peers.size)
  private var initialized = false
  private val kvBatch = new java.util.ArrayList[ImportSstpb.Pair]()
  private var kvBatchSizeInBytes = 0

  private class WriteClient(asyncStub: ImportSSTGrpc.ImportSSTStub) {
    private var writeError: Throwable = _
    private var writeResp: ImportSstpb.WriteResponse = _
    private val finishLatch = new CountDownLatch(1)

    private val respObserver = new StreamObserver[ImportSstpb.WriteResponse] {
      override def onNext(resp: ImportSstpb.WriteResponse): Unit = {
        writeResp = resp
      }

      override def onError(t: Throwable): Unit = {
        writeError = t
        finishLatch.countDown()
      }

      override def onCompleted(): Unit = {
        finishLatch.countDown()
      }
    }

    private val reqObserver = asyncStub.write(respObserver)

    def writeMeta(meta: ImportSstpb.SSTMeta): Unit = {
      reqObserver.onNext(ImportSstpb.WriteRequest.newBuilder().setMeta(meta).build())
    }

    def writeBatch(batch: ImportSstpb.WriteBatch): Unit = {
      reqObserver.onNext(ImportSstpb.WriteRequest.newBuilder().setBatch(batch).build())
    }

    def finishWrite(): java.util.List[SSTMeta] = {
      reqObserver.onCompleted()
      finishLatch.await()
      if (writeError != null) {
        throw writeError
      }
      if (writeResp.hasError) {
        throw new RuntimeException(writeResp.getError.getMessage)
      }
      writeResp.getMetasList
    }
  }

  private def lazyInit(): Unit = {
    if (!initialized) {
      for (i <- peers.indices) {
        storeAddrs(i) = pdClient.getStore(peers(i).getStoreId).getAddress
        val channel = ChannelPool.getChannel(storeAddrs(i))
        writeClients(i) = new WriteClient(ImportSSTGrpc.newStub(channel))
      }
      val meta = ImportSstpb.SSTMeta.newBuilder()
        .setUuid(ByteString.copyFrom(genUUID()))
        .setRegionId(region.meta.getId)
        .setRegionEpoch(region.meta.getRegionEpoch)
        .build()
      writeClients.foreach(client => {
        client.writeMeta(meta)
      })
      initialized = true
    }
  }

  private def flush(): Unit = {
    val writeBatch = ImportSstpb.WriteBatch.newBuilder()
      .addAllPairs(kvBatch)
      .setCommitTs(pdClient.getTS)
      .build()
    writeClients.foreach(client => {
      client.writeBatch(writeBatch)
    })
    kvBatch.clear()
    kvBatchSizeInBytes = 0
  }

  def write(key: Array[Byte], value: Array[Byte]): Unit = {
    lazyInit()
    if (kvBatch.size() >= MAX_KV_BATCH_KEYS ||
      kvBatchSizeInBytes + key.length + value.length > MAX_KV_BATCH_SIZE_IN_BYTES) {
      flush()
    }
    kvBatch.add(ImportSstpb.Pair.newBuilder()
      .setKey(ByteString.copyFrom(key))
      .setValue(ByteString.copyFrom(value))
      .build()
    )
  }

  def finish(): Unit = {
    lazyInit()
    flush()
    val sstMetas = writeClients.map(client => client.finishWrite()).apply(0)
    var leaderAddr: String = null
    for (i <- peers.indices) {
      if (peers(i).getId == region.leader.getId) {
        leaderAddr = storeAddrs(i)
      }
    }
    val leaderChannel = ChannelPool.getChannel(leaderAddr)
    for (addr <- storeAddrs) {
      ChannelPool.releaseChannel(addr)
    }

//    val ingestReq = ImportSstpb.MultiIngestRequest.newBuilder()
//      .setContext(region.getLeaderContext)
//      .addAllSsts(metas)
//      .build()
//    val blockingStub = ImportSSTGrpc.newBlockingStub(leaderChannel)
//    val resp = blockingStub.multiIngest(ingestReq)
//    if (resp.hasError) {
//      // TODO: handle error and retry.
//      throw new RegionException(resp.getError)
//    }
  }
}

object ImportClient {
  private val MAX_KV_BATCH_KEYS = 1024 * 32
  private val MAX_KV_BATCH_SIZE_IN_BYTES = 1024 * 1024

  private def genUUID(): Array[Byte] = {
    val uuid = UUID.randomUUID()
    val out = new Array[Byte](16)
    val msb = uuid.getMostSignificantBits
    val lsb = uuid.getLeastSignificantBits
    for (i <- 0 until 8) {
      out(i) = ((msb >> ((7 - i) * 8)) & 0xff).toByte
    }
    for (i <- 8 until 16) {
      out(i) = ((lsb >> ((15 - i) * 8)) & 0xff).toByte
    }
    out
  }
}
