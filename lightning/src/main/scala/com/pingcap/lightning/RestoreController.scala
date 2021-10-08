package com.pingcap.lightning

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.tikv.common.TiConfiguration
import org.tikv.common.meta.TiTableInfo
import org.tikv.shade.com.fasterxml.jackson.databind.ObjectMapper

import scalaj.http.Http
import java.sql.DriverManager

class RestoreController(conf: Config, sparkConf: SparkConf) {
  private val tiConf = TiConfiguration.createDefault(conf.getPDAddr)
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  def restoreAll(): Unit = {
    val dataSource = new DataSource(spark.sparkContext, conf.getSourceDir)
    for (db <- dataSource.getDatabases) {
      restoreDatabase(db)
    }
  }

  private def parseSQLFile(sqlFile: Path): String = {
    spark.sparkContext.textFile(sqlFile.toString)
      .filter(line => !line.startsWith("--")).collect().mkString
      .replace('\n', ' ')
      .replace('\r', ' ')
      .replaceAll("/\\*!.*?\\*/", "")
      .replaceAll("^[ \t;]*", "")
      .replaceAll("[ \t;]*$", "")
  }

  private def restoreDatabase(db: DatabaseMeta): Unit = {
    val url = s"jdbc:mysql://${conf.getTiDBAddr}"
    val driver = "com.mysql.cj.jdbc.Driver"
    Class.forName(driver)

    if (db.schemaFile != null) {
      val dbConn = DriverManager.getConnection(url, conf.getTiDBUser, conf.getTiDBPassword)
      try {
        val sql = parseSQLFile(db.schemaFile)
        dbConn.createStatement().execute(sql)
      } finally {
        dbConn.close()
      }
    }
    if (db.tables.exists(t => t.schemaFile != null)) {
      val dbConn = DriverManager.getConnection(s"$url/${db.name}", "root", "")
      try {
        for (table <- db.tables) {
          val sql = parseSQLFile(table.schemaFile)
          dbConn.createStatement().execute(sql)
        }
      } finally {
        dbConn.close()
      }
    }
    for (table <- db.tables) {
      restoreTable(db.name, table)
    }
  }

  private def fetchTableInfo(dbName: String, tableName: String): TiTableInfo = {
    //noinspection HttpUrlsUsage
    val url = s"http://${conf.getTiDBStatusAddr}/schema/$dbName/$tableName"
    val resp = Http(url).asString
    if (resp.code != 200) {
      throw new RuntimeException(s"fetch table info of `$dbName`.`$tableName`" +
        s" received unexpected status code ${resp.code} body ${resp.body}`")
    }
    val mapper = new ObjectMapper()
    mapper.readValue(resp.body, classOf[TiTableInfo])
  }

  private def restoreTable(dbName: String, table: TableMeta): Unit = {
    val tableInfo = fetchTableInfo(dbName, table.name)
    val dataFiles = table.dataFiles.map(path => path.toString).toArray
    val rdd = spark.read.csv(dataFiles: _*).rdd
    val tr = new TableRestore(tiConf)
    tr.restore(tableInfo, rdd)
  }
}
