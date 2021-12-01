package com.pingcap.lightning

import com.google.protobuf.ByteString
import com.pingcap.kvproto.ImportSstpb.SSTMeta
import com.pingcap.kvproto.Metapb.RegionEpoch
import com.pingcap.kvproto.{ImportSSTGrpc, ImportSstpb, Kvrpcpb}
import com.pingcap.lightning.ImportClient._
import io.grpc.stub.StreamObserver

import java.util.UUID
import java.util.concurrent.CountDownLatch

class ImportClient(
  pdClient: PDClient,
  regionID: Long,
  regionEpoch: RegionEpoch,
  leaderContext: Kvrpcpb.Context,
  leaderAddr: String,
  peerAddrs: Array[String],
  channelPool: SharedChannelPool
) {
  private var initialized = false
  private val writeClients = new Array[WriteClient](peerAddrs.length)

  private val kvBatch = new java.util.ArrayList[ImportSstpb.Pair]()
  private var kvBatchSizeInBytes = 0

  private class WriteClient(channel: SharedChannel) {
    private val asyncStub = ImportSSTGrpc.newStub(channel)
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

    def finish(): java.util.List[SSTMeta] = {
      reqObserver.onCompleted()
      finishLatch.await()
      channel.close()
      if (writeError != null) {
        throw writeError
      }
      if (writeResp.hasError) {
        throw new RuntimeException(writeResp.getError.getMessage)
      }
      writeResp.getMetasList
    }

    def abort(): Unit = {
      channel.close()
    }
  }

  private def lazyInit(): Unit = {
    if (!initialized) {
      for (i <- peerAddrs.indices) {
        writeClients(i) = new WriteClient(channelPool.getChannel(peerAddrs(i)))
      }
      val meta = ImportSstpb.SSTMeta.newBuilder()
        .setUuid(ByteString.copyFrom(genUUID()))
        .setRegionId(regionID)
        .setRegionEpoch(regionEpoch)
        .build()
      writeClients.foreach(client => {
        client.writeMeta(meta)
      })
      initialized = true
    }
  }

  private def flush(): Unit = {
    lazyInit()
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
    flush()
    val writeResults = writeClients.map(client => client.finish()).toList
    for (i <- 1 until writeResults.length) {
      assert(writeResults(i).equals(writeResults.head))
    }
    val sstMetas = writeResults.head
    val req = ImportSstpb.MultiIngestRequest.newBuilder()
      .setContext(leaderContext)
      .addAllSsts(sstMetas)
      .build()
    val channel = channelPool.getChannel(leaderAddr)
    val blockingStub = ImportSSTGrpc.newBlockingStub(channel)
    try {
      val resp = blockingStub.multiIngest(req)
      if (resp.hasError) {
        // TODO: handle error and retry.
        throw new RuntimeException(resp.getError.getMessage)
      }
    } finally {
      channel.close()
    }
  }

  def abort(): Unit = {
    writeClients.foreach(client => client.abort())
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
