package com.pingcap.lightning

import com.pingcap.kvproto.Metapb

class Region {
  var meta: Metapb.Region = _
  var leader: Metapb.Peer = _
  var downPeers: Array[Metapb.Peer] = _
  var pendingPeers: Array[Metapb.Peer] = _

  def setMeta(meta: Metapb.Region): Region = {
    this.meta = meta
    this
  }

  def setLeader(leader: Metapb.Peer): Region = {
    this.leader = leader
    this
  }

  def setDownPeers(downPeers: Array[Metapb.Peer]): Region = {
    this.downPeers = downPeers
    this
  }

  def setPendingPeers(pendingPeers: Array[Metapb.Peer]): Region = {
    this.pendingPeers = pendingPeers
    this
  }
}
