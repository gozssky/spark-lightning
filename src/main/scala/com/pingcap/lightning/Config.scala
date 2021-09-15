package com.pingcap.lightning

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.asScalaSetConverter

class Config(loadDefaults: Boolean) extends Cloneable with Serializable {
  private val settings = new ConcurrentHashMap[String, String]()

  def this() = this(true)

  if (loadDefaults) {
    loadFromSystemProperties()
    loadFromDefaultProperties()
  }

  private def loadFromSystemProperties(): Unit = {
    val props = System.getProperties.stringPropertyNames().asScala
      .map(key => (key, System.getProperty(key))).toMap
    for ((key, value) <- props if key.startsWith("lightning.")) {
      set(key, value)
    }
  }

  private def loadFromDefaultProperties(): Unit = {
    setIfMissing("lightning.tidb.host", "127.0.0.1")
    setIfMissing("lightning.tidb.port", "4000")
    setIfMissing("lightning.tidb.status-port", "10080")
    setIfMissing("lightning.tidb.pd-addr", "127.0.0.1:2379")
    setIfMissing("lightning.tidb.user", "root")
  }

  private def checkSet(key: String, value: String): Unit = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
  }

  def set(key: String, value: String): Config = {
    checkSet(key, value)
    settings.put(key, value)
    this
  }

  def setIfMissing(key: String, value: String): Config = {
    checkSet(key, value)
    settings.putIfAbsent(key, value)
    this
  }

  def setSourceDir(dir: String): Config = {
    set("lightning.source.data-source-dir", dir)
    this
  }

  def setTiDBHost(host: String): Config = {
    set("lightning.tidb.host", host)
    this
  }

  def setTiDBStatusHost(host: String): Config = {
    set("lightning.tidb.status-host", host)
    this
  }

  def setTiDBPort(port: Int): Config = {
    set("lightning.tidb.port", port.toString)
    this
  }

  def setTiDBStatusPort(port: Int): Config = {
    set("lightning.tidb.status-port", port.toString)
  }

  def setPDAddr(addr: String): Config = {
    set("lightning.tidb.pd-addr", addr)
    this
  }

  def setTiDBUser(user: String): Config = {
    set("lightning.tidb.user", user)
    this
  }

  def setTiDBPassword(password: String): Config = {
    set("lightning.tidb.password", password)
    this
  }

  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException)
  }

  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  def getSourceDir: String = {
    get("lightning.source.data-source-dir")
  }

  def getTiDBAddr: String = {
    val host = get("lightning.tidb.host")
    val port = get("lightning.tidb.port")
    s"$host:$port"
  }

  def getTiDBStatusAddr: String = {
    val statusHost = get("lightning.tidb.status-host", get("lightning.tidb.host"))
    val statusPort = get("lightning.tidb.status-port")
    s"$statusHost:$statusPort"
  }

  def getPDAddr: String = {
    get("lightning.tidb.pd-addr")
  }

  def getTiDBUser: String = {
    get("lightning.tidb.user")
  }

  def getTiDBPassword: String = {
    get("lightning.tidb.password", "")
  }
}
