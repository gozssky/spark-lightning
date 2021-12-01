package com.pingcap.lightning

import com.pingcap.kvproto.Metapb

class TiRegion {
  var meta: Metapb.Region = _
  var leader: Metapb.Peer = _
  var downPeers: Array[Metapb.Peer] = _
  var pendingPeers: Array[Metapb.Peer] = _

  def setMeta(meta: Metapb.Region): TiRegion = {
    this.meta = meta
    this
  }

  def setLeader(leader: Metapb.Peer): TiRegion = {
    this.leader = leader
    this
  }

  def setDownPeers(downPeers: Array[Metapb.Peer]): TiRegion = {
    this.downPeers = downPeers
    this
  }

  def setPendingPeers(pendingPeers: Array[Metapb.Peer]): TiRegion = {
    this.pendingPeers = pendingPeers
    this
  }
}
