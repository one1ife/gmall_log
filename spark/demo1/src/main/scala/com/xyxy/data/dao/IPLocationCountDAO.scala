package com.xyxy.data.dao

import com.xyxy.data.bean.IPLocationCount
import com.xyxy.data.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


object IPLocationCountDAO {

  val tableName = "ip"
  val cf = "info"
  val qualifer = "click_count"

  /**
   * 保存数据
   * @param list
   */
  def save(list:ListBuffer[IPLocationCount]): Unit ={
    val table =  HBaseUtils.getInstance().getTable(tableName)
    for(els <- list){
      table.incrementColumnValue(Bytes.toBytes(els.Location),Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.Count);
    }

  }

  def count(day_categary:String) : Long={
    val table  =HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_categary))
    val  value =  table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
    if(value == null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[IPLocationCount]
    //list.append(CategarySearchClickCount("20171122_1_1",300))
    list.append(IPLocationCount("美国", 300))
    list.append(IPLocationCount("英国", 1600))
    save(list)

    print(count("美国") + "---" )
  }

}
