package com.xyxy.data

import com.xyxy.data.bean._
import com.xyxy.data.dao.{BrandCountDAO, CategaryClickCountDAO, IPLocationCountDAO, VisitCountDAO}
import com.xyxy.data.utils.{DataUtils, IPLocationDemo}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object StatStreamingApp {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(conf, Seconds(10))

    // master为linux主机名，如果指定为master，并且在idea直接运行
    // 则需要在C:\Windows\System32\drivers\etc\hosts下添加主机名和linux ip的映射
    // 或者直接指定 linux 的ip也可以
    val brokers = "ubuntu20:9092"
    val topics = Array("flumeTopic")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val logs = KafkaUtils
      .createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      ).map(_.value())

    //数据清洗，生成实体类集合
    //116.58.208.104	2022-08-28 16:27:13	"GET laptop/821 HTTP/1.0"	https://cn.bing.com/search?key=小米	200
    var cleanlog = logs.map(line => {
      var infos = line.split("\t")
      var url = infos(2).split(" ")(1)
      var categaryId = 0;
      if (url.startsWith("gmall")) {
        categaryId = url.split("/")(1).toInt
      }

      ClickLog(infos(0), DataUtils.parseToMin(infos(1)), categaryId, infos(3), infos(4).toInt)

    }).filter(log => {
      log.categoryId != 0;
    })


    cleanlog.print()

    //每个类别每天的点击量
    cleanlog.map(log => {
      (log.time.substring(0, 8) + "_" + log.categoryId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitions => {
        val list = new ListBuffer[CategaryClickCount]
        partitions.foreach(pair => {
          list.append(CategaryClickCount(pair._1, pair._2))
        })
        CategaryClickCountDAO.save(list)
      })
    })

    //每个栏目下面从渠道过来的流量
    cleanlog.map(log => {
      val url = log.refer.replace("//", "/")
      val splits = url.split("/")
      var host = "domin"
      if (splits.length > 2) {
        host = splits(1)
      }
      (log.time.substring(0, 8) + "_" + host, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      val list = new ListBuffer[VisitCount]
      rdd.foreachPartition(partions => {
        partions.foreach(pairs => {
          list.append(VisitCount(pairs._1, pairs._2))
        })
        VisitCountDAO.save(list)
      })
    })


    //访问ip
    cleanlog.map(log => {
      val address = IPLocationDemo.getAddress(log.ip)
      (log.time.substring(0, 8), address, 1)
    }).filter(x => x._2 != "").map(x => {
      (x._1 + "_" + x._2, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitions => {
        val list = new ListBuffer[IPLocationCount]
        partitions.foreach(pair => {
          list.append(IPLocationCount(pair._1, pair._2))
        })
        IPLocationCountDAO.save(list)
      })
    })

    //搜索品牌关键词
    cleanlog.map(log => {
      val url = log.refer.split("=")
      var key = ""
      if (url.length > 1) {
        key = url(1)
      }
      (log.time.substring(0, 8), key, 1)
    }).filter(x => x._2 != "").map(x => {
      (x._1 + "_" + x._2, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      val list = new ListBuffer[BrandCount]
      rdd.foreachPartition(partions => {
        partions.foreach(pairs => {
          list.append(BrandCount(pairs._1, pairs._2))
        })
        BrandCountDAO.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
