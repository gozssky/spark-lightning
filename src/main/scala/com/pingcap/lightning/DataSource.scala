package com.pingcap.lightning

import org.apache.spark
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class DatabaseMeta(name: String, var schemaFile: Path, tables: ArrayBuffer[TableMeta]) extends Serializable

case class TableMeta(name: String, var schemaFile: Path, dataFiles: ArrayBuffer[Path]) extends Serializable

class DataSource(sc: spark.SparkContext, sourceDir: String) {
  private val databases = new ArrayBuffer[DatabaseMeta]()

  private val dbIndexMap = new mutable.HashMap[String, Int]()
  private val tableIndexMap = new mutable.HashMap[(String, String), Int]()

  loadMetaFromSource()

  private def addDBSchemaFile(dbName: String, schemaFile: Path): Unit = {
    if (!dbIndexMap.contains(dbName)) {
      databases.append(DatabaseMeta(dbName, schemaFile, new ArrayBuffer[TableMeta]()))
      dbIndexMap.put(dbName, databases.length - 1)
    }
    val db = databases(dbIndexMap(dbName))
    if (db.schemaFile == null) {
      db.schemaFile = schemaFile
    }
  }

  private def addTableSchemaFile(dbName: String, tableName: String, schemaFile: Path): Unit = {
    addDBSchemaFile(dbName, null)
    val db = databases(dbIndexMap(dbName))
    if (!tableIndexMap.contains((dbName, tableName))) {
      db.tables.append(TableMeta(tableName, schemaFile, new ArrayBuffer[Path]()))
      tableIndexMap.put((dbName, tableName), db.tables.length - 1)
    }
    val table = db.tables(tableIndexMap(dbName, tableName))
    if (table.schemaFile == null) {
      table.schemaFile = schemaFile
    }
  }

  private def addTableDataFile(dbName: String, tableName: String, schemaFile: Path): Unit = {
    addDBSchemaFile(dbName, null)
    addTableSchemaFile(dbName, tableName, null)
    val table = databases(dbIndexMap(dbName)).tables(tableIndexMap((dbName, tableName)))
    table.dataFiles.append(schemaFile)
  }

  private def loadMetaFromSource(): Unit = {
    val uri = URI.create(sourceDir)
    val fsURI = if (uri.getScheme != null && uri.getScheme.startsWith("s3")) {
      val rawQuery = uri.getQuery
      val query = rawQuery.split('&').map(_.split('='))
        .filter(_.length == 2).map(v => (v(0), v(1))).toMap
      sc.hadoopConfiguration.setIfUnset("fs.s3a.access.key", query.getOrElse("access-key", ""))
      sc.hadoopConfiguration.setIfUnset("fs.s3a.secret.key", query.getOrElse("secret-access-key", ""))
      sc.hadoopConfiguration.setIfUnset("fs.s3a.endpoint", query.getOrElse("endpoint", ""))
      var urlStr = s"s3a://${uri.getHost}${uri.getPath}"
      if (!urlStr.endsWith("/")) {
        urlStr += '/'
      }
      URI.create(urlStr)
    } else {
      URI.create(s"file://${uri.getHost}${uri.getPath}")
    }
    val fs = FileSystem.get(fsURI, sc.hadoopConfiguration)
    val iter = fs.listFiles(new Path(fsURI), true)
    while (iter.hasNext) {
      val fileStatus = iter.next
      if (fileStatus.isFile) {
        val filePath = fileStatus.getPath
        val fileName = filePath.getName
        if (fileName.endsWith("-schema-create.sql")) {
          val dbName = fileName.substring(0, fileName.length - "-schema-create.sql".length)
          addDBSchemaFile(dbName, filePath)
        } else if (fileName.endsWith("-schema.sql")) {
          val idx = fileName.indexOf(".")
          if (idx > 0) {
            val dbName = fileName.substring(0, idx)
            val tableName = fileName.substring(idx + 1, fileName.length - "-schema.sql".length)
            addTableSchemaFile(dbName, tableName, filePath)
          }
        } else if (fileName.endsWith(".csv")) {
          val parts = fileName.split("\\.")
          if (parts.length > 2) {
            val dbName = parts(0)
            val tableName = parts(1)
            addTableDataFile(dbName, tableName, filePath)
          }
        }
      }
    }
  }

  def getDatabases: ArrayBuffer[DatabaseMeta] = {
    databases
  }
}
