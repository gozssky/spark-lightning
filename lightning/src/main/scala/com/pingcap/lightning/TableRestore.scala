package com.pingcap.lightning

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.common.codec.{CodecDataOutput, TableCodec}
import org.tikv.common.importer.SwitchTiKVModeClient
import org.tikv.common.key.{IndexKey, Key, RowKey}
import org.tikv.common.meta.{TiIndexInfo, TiTableInfo}
import org.tikv.common.region.TiRegion
import org.tikv.common.row.ObjectRowImpl

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class TableRestore(tiConf: TiConfiguration) extends Serializable {
  private type TiRow = org.tikv.common.row.Row

  def restore(tableInfo: TiTableInfo, rdd: RDD[Row]): Unit = {
    val tableID = tableInfo.getId

    val tiRows = rdd.map(row => encodeToTiRow(tableInfo, row))
    val tiRowHandlePairs = if (tableInfo.isPkHandle) {
      val pkColumn = tableInfo.getPKIsHandleColumn
      tiRows.map(row => {
        (row, row.get(pkColumn.getOffset, pkColumn.getType).asInstanceOf[Long])
      })
    } else {
      tiRows.zipWithIndex().map(r => (r._1, r._2 + 1))
    }

    val dataKVs = tiRowHandlePairs.map(pair => {
      val tiRow = pair._1
      val handle = pair._2
      val rowKey = RowKey.toRowKey(tableID, handle).getBytes
      val columns = tableInfo.getColumns
      val values = new Array[AnyRef](columns.size())
      for (i <- 0 until tableInfo.getColumns.size()) {
        values.update(i, tiRow.get(i, columns.get(i).getType))
      }
      val encodedValue = TableCodec.encodeRow(tableInfo.getColumns, values, tableInfo.isPkHandle, true)
      (rowKey, encodedValue)
    })
    val indexKVs = tableInfo.getIndices.asScala.flatMap(indexInfo =>
      if (tableInfo.isPkHandle && indexInfo.isPrimary) {
        None
      } else {
        Some(tiRowHandlePairs.map(pair => {
          val tiRow = pair._1
          val handle = pair._2
          val pair2 = encodeIndexKV(tableInfo, indexInfo, tiRow, handle)
          (pair2._1, pair2._2)
        }))
      }
    ).toArray

    restoreKVs(dataKVs, !tableInfo.isPkHandle)
    indexKVs.foreach(kvs => restoreKVs(kvs, isKVSorted = false))
  }

  private def restoreKVs(kvs: RDD[(Array[Byte], Array[Byte])], isKVSorted: Boolean): Unit = {
    val sortedKVs = if (isKVSorted) {
      kvs
    } else {
      kvs.sortBy(kv => Key.toRawKey(kv._1))
    }.persist(StorageLevel.DISK_ONLY)

    val totalKVCount = sortedKVs.count()
    val splitPoints = sortedKVs.zipWithIndex()
      .filter(row => row._2 % 960000 == 0)
      .map(row => row._1._1)
      .collect()
    val splitKeys = splitPoints.toList.asJava

    val tiSession = TiSession.create(tiConf)
    val switchTiKVModeClient = new SwitchTiKVModeClient(tiSession.getPDClient, tiSession.getImporterRegionStoreClientBuilder)
    switchTiKVModeClient.switchTiKVToNormalMode()
    tiSession.splitRegionAndScatter(splitKeys, 3600000, 3600000, 3600000)

    val minKey = Key.toRawKey(sortedKVs.first()._1)
    val maxKey = Key.toRawKey(sortedKVs.zipWithIndex().filter(row => row._2 == totalKVCount - 1).first()._1._1)
    val regions = new ArrayBuffer[TiRegion]()
    tiSession.getRegionManager.invalidateAll()
    var curKey = minKey
    while (curKey.compareTo(maxKey) <= 0) {
      val region = tiSession.getRegionManager.getRegionByKey(curKey.toByteString)
      curKey = Key.toRawKey(region.getEndKey)
      regions.append(region)
    }

    val partitioner = new RegionPartitioner(regions.toArray)
    sortedKVs
      .partitionBy(partitioner)
      .foreachPartition(iter => {
        writeAndIngest(iter, partitioner)
      })

    switchTiKVModeClient.stopKeepTiKVToImportMode()
    switchTiKVModeClient.switchTiKVToNormalMode()
    tiSession.close()
  }

  private def writeAndIngest(iter: Iterator[(Array[Byte], Array[Byte])], partitioner: RegionPartitioner): Unit = {
    //    if (iter.nonEmpty) {
    //      val region = partitioner.getPartitionRegion(TaskContext.getPartitionId())
    //      if (region != null) {
    //        val tiSession = TiSession.create(tiConf)
    //        val importClient = new ImportClient(tiSession, region)
    //        importClient.write(iter)
    //        tiSession.close()
    //      }
    //    }
  }

  // TODO: This method may be used later.
  //  private def encodeTxnKey(key: Array[Byte]): Array[Byte] = {
  //    val cdo = new CodecDataOutput()
  //    Codec.BytesCodec.writeBytes(cdo, key)
  //    cdo.toBytes
  //  }

  private def encodeToTiRow(tableInfo: TiTableInfo, row: Row): TiRow = {
    val columns = tableInfo.getColumns
    assert(columns.size == columns.size())
    val tiRow = ObjectRowImpl.create(columns.size())
    for (i <- 0 until columns.size()) {
      // TODO: handle quote and escape.
      val field = row(i) match {
        case str: String if str == "NULL" =>
          null
        case other => other
      }
      tiRow.set(i, columns.get(i).getType, columns.get(i).getType.convertToTiDBType(field))
    }
    tiRow
  }

  private def encodeIndexKV(tableInfo: TiTableInfo, indexInfo: TiIndexInfo, tiRow: TiRow, handle: Long): (Array[Byte], Array[Byte]) = {
    val encodeResult = IndexKey.encodeIndexDataValues(
      tiRow, indexInfo.getIndexColumns, handle, indexInfo.isUnique, tableInfo)
    val indexKey = IndexKey.toIndexKey(tableInfo.getId, indexInfo.getId, encodeResult.keys: _*)
    val encodedKey = if (indexInfo.isUnique) {
      indexKey.getBytes
    } else {
      val cdo = new CodecDataOutput()
      cdo.write(indexKey.getBytes)
      cdo.writeLong(handle)
      cdo.toBytes
    }
    val encodedValue = if (indexInfo.isUnique && !encodeResult.appendHandle) {
      val cdo = new CodecDataOutput()
      cdo.writeLong(handle)
      cdo.toBytes
    } else {
      Array[Byte] {
        '0'
      }
    }
    (encodedKey, encodedValue)
  }
}
