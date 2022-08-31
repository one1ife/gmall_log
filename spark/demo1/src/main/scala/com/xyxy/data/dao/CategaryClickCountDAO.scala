package com.xyxy.data.dao

import com.xyxy.data.bean.CategaryClickCount
import com.xyxy.data.utils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CategaryClickCountDAO {

  val tableName = "category_clickcount"
  val cf = "info"
  val clumn = "click_count"

  /*
  保存数据
   */
  def save(list:ListBuffer[CategaryClickCount]) : Unit ={
    val table = HBaseUtils.getInstance().getTable(tableName)
    for (els <- list){
      table.incrementColumnValue(Bytes.toBytes(els.categoryID),Bytes.toBytes(cf),Bytes.toBytes(clumn),els.clickCount)
    }
  }

  def count(day_category:String) : Long={
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_category))
    val values = table.get(get).getValue(Bytes.toBytes(cf),Bytes.toBytes(clumn))
      if(values == null){
      0L
      }else{
        Bytes.toLong(values)
      }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CategaryClickCount]
    list.append(CategaryClickCount("20220828_8",300))
    list.append(CategaryClickCount("20220828_9",500))
    save(list)

    print(count("20220828_8") + "----" + count("20220828_9"))
  }

}
