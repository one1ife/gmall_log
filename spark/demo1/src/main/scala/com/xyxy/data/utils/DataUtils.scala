package com.xyxy.data.utils

import org.apache.commons.lang3.time.FastDateFormat

import java.util.Date

object DataUtils {
  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TAG_FORMAT = FastDateFormat.getInstance("yyyyMMdd")

  //把当前时间转换为时间戳
  def getTime(time: String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMin(time: String) = {
    TAG_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    print(parseToMin("2017-11-20 00:39:26"))
  }

}
