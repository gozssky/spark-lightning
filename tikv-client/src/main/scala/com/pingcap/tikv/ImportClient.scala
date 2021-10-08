package com.pingcap.tikv

import com.pingcap.kvproto.Metapb

import scala.collection.JavaConverters._

class ImportClient(pdAddr: String, region: Metapb.Region) {
  private val peers = region.getPeersList.asScala

  //  private val channels = new Array[ManagedChannel](peers.size)
  //  private val writeClients = new Array[WriteClient](peers.size)

  //  region.getPeers(0).

  //  for (i <- peers.indices) {
  //
  //    val store = tiSession.getRegionManager.getStoreById(peers(i).getStoreId)
  //    channels(i) = tiSession.getChannelFactory.getChannel(store.getAddress, tiSession.getPDClient.getHostMapping)
  //    writeClients(i) = new WriteClient(ImportSSTGrpc.newStub(channels(i)))
  //  }
  //
  //  class WriteClient(asyncStub: ImportSSTGrpc.ImportSSTStub) {
  //    private var writeError: Throwable = _
  //    private var writeResp: ImportSstpb.WriteResponse = _
  //    private val finishLatch = new CountDownLatch(1)
  //
  //    private val respObserver = new StreamObserver[ImportSstpb.WriteResponse] {
  //      override def onNext(resp: ImportSstpb.WriteResponse): Unit = {
  //        writeResp = resp
  //      }
  //
  //      override def onError(t: Throwable): Unit = {
  //        writeError = t
  //        finishLatch.countDown()
  //      }
  //
  //      override def onCompleted(): Unit = {
  //        finishLatch.countDown()
  //      }
  //    }
  //
  //    private val reqObserver = asyncStub.write(respObserver)
  //
  //    def writeMeta(meta: ImportSstpb.SSTMeta): Unit = {
  //      reqObserver.onNext(ImportSstpb.WriteRequest.newBuilder().setMeta(meta).build())
  //    }
  //
  //    def writeBatch(batch: ImportSstpb.WriteBatch): Unit = {
  //      reqObserver.onNext(ImportSstpb.WriteRequest.newBuilder().setBatch(batch).build())
  //    }
  //
  //    def finishWrite(): util.List[SSTMeta] = {
  //      reqObserver.onCompleted()
  //      finishLatch.await()
  //      if (writeError != null) {
  //        throw writeError
  //      }
  //      if (writeResp.hasError) {
  //        throw new RuntimeException(writeResp.getError.getMessage)
  //      }
  //      writeResp.getMetasList
  //    }
  //  }
  //
  //  private val maxKVBatchSize = 1024 * 32
  //  private val maxKVBatchBytes = 1024 * 1024
  //
  //  def write(kvIter: Iterator[(Array[Byte], Array[Byte])]): Unit = {
  //    if (kvIter.isEmpty) {
  //      return
  //    }
  //    //    val meta = ImportSstpb.SSTMeta.newBuilder()
  //    //      .setUuid(ByteString.copyFrom(genUUID()))
  //    //      .setRegionId(region.getId)
  //    //      .setRegionEpoch(region.getRegionEpoch)
  //    //      .build()
  //    //    writeClients.foreach(client => {
  //    //      client.writeMeta(meta)
  //    //    })
  //
  //    val kvBatch = new util.ArrayList[ImportSstpb.Pair]()
  //    var totalBytes = 0
  //
  //    def flush(): Unit = {
  //      val writeBatch = ImportSstpb.WriteBatch.newBuilder()
  //        .addAllPairs(kvBatch)
  //        .setCommitTs(tiSession.getTimestamp.getVersion)
  //        .build()
  //      writeClients.foreach(client => {
  //        client.writeBatch(writeBatch)
  //      })
  //      kvBatch.clear()
  //      totalBytes = 0
  //    }
  //
  //    while (kvIter.hasNext) {
  //      val pair = kvIter.next()
  //      val bytes = pair._1.length + pair._2.length
  //      if (kvBatch.size() >= maxKVBatchSize || totalBytes + bytes > maxKVBatchBytes) {
  //        flush()
  //      }
  //      //      kvBatch.add(ImportSstpb.Pair.newBuilder()
  //      //        .setKey(ByteString.copyFrom(pair._1))
  //      //        .setValue(ByteString.copyFrom(pair._2))
  //      //        .build()
  //      //      )
  //    }
  //    if (!kvBatch.isEmpty) {
  //      flush()
  //    }
  //
  //    val metas = writeClients.map(client => client.finishWrite()).apply(0)
  //    val leaderId = region.getLeader.getId
  //    var leaderChannel: ManagedChannel = null
  //    for (i <- peers.indices) {
  //      if (peers(i).getId == leaderId) {
  //        leaderChannel = channels(i)
  //      }
  //    }
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
  //  }
  //
  //  private def genUUID(): Array[Byte] = {
  //    val uuid = UUID.randomUUID()
  //    val out = new Array[Byte](16)
  //    val msb = uuid.getMostSignificantBits
  //    val lsb = uuid.getLeastSignificantBits
  //    for (i <- 0 until 8) {
  //      out(i) = ((msb >> ((7 - i) * 8)) & 0xff).toByte
  //    }
  //    for (i <- 8 until 16) {
  //      out(i) = ((lsb >> ((15 - i) * 8)) & 0xff).toByte
  //    }
  //    out
  //  }
}
