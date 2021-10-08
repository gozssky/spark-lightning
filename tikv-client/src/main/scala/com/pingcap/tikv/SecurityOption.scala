package com.pingcap.tikv

import com.pingcap.tikv.SecurityOption.empty_
import io.netty.handler.ssl.{SslContext, SslContextBuilder}

import java.io.File

case class SecurityOption(trustCertCollectionPath: String, keyCertChainPath: String, keyPath: String) {
  def toSslContext: SslContext = {
    if (this == empty_) {
      return null
    }
    val builder = SslContextBuilder.forClient()
    if (trustCertCollectionPath != null) {
      builder.trustManager(new File(trustCertCollectionPath))
    }
    if (keyCertChainPath != null && keyPath != null) {
      builder.keyManager(new File(keyCertChainPath), new File(keyPath))
    }
    builder.build()
  }
}

object SecurityOption {
  private val empty_ = new SecurityOption(null, null, null)

  def insecure: SecurityOption = {
    empty_
  }
}
