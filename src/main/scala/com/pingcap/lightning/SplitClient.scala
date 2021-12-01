package com.pingcap.lightning

import com.google.protobuf.ByteString
import com.pingcap.kvproto.Metapb
import io.grpc.ForwardingChannelBuilder
import io.grpc.internal.ManagedChannelImplBuilder

import java.util
import scala.collection.mutable.ArrayBuffer

class SplitClient(pdClient: PDClient) {
  def splitRegionAndScatter(splitKeys: Seq[Array[Byte]]): Unit = {
    //    val regions = splitRegion(splitKeys.map(key => ByteString.copyFrom(key)).asJava)
    //    val pdClient = tiSession.getPDClient
    //    for (region <- regions) {
    //      tiSession.getConf.getPdAddrs
    //      //      pdClient.
    //    }
  }

  private def splitRegion(splitKeys: util.List[ByteString]): ArrayBuffer[Metapb.Region] = {
    //    val regionManager = tiSession.getRegionManager
    //    val backOffer = ConcreteBackOffer.newCustomBackOff(60000)
    //    val groupKeys = ClientUtils.groupKeysByRegion(regionManager, splitKeys, backOffer)
    //    val newRegions = new ArrayBuffer[Metapb.Region]()
    //    for ((region, keys) <- groupKeys.asScala) {
    //      assert(!region.getPeersList.isEmpty)
    //      val regionSplitKeys = keys.asScala
    //        .filter(key => !key.equals(region.getStartKey) && !key.equals(region.getEndKey))
    //        .toList.asJava
    //      if (!regionSplitKeys.isEmpty) {
    //        val peer = if (region.getLeader != null && region.getLeader.getId != 0) {
    //          region.getLeader
    //        } else {
    //          region.getPeersList.get(0)
    //        }
    //        val store = regionManager.getStoreById(peer.getStoreId)
    //        val storeClient = tiSession.getRegionStoreClientBuilder.build(region, store)
    //        val regions = storeClient.splitRegion(regionSplitKeys).asScala
    //        regionManager.invalidateRegion(region)
    //        newRegions.append(regions: _*)
    //      }
    //    }
    //    newRegions
    null
  }
}
