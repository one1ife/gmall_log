package com.xyxy.data.utils

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import scala.collection.mutable.ArrayBuffer

/**
 * Describe: IP归属地查询工具类
 * Author:   jackyzou
 * Data:     202/08/29.
 */
object IPLocationDemo {

  def main(args: Array[String]) = {
    val ip = "122.144.128.133"
    val address = getAddress(ip)
    print(address)

  }

  //获取省级归属地方法，通过查询ip.txt字典
  def getAddress(ip: String):String = {
    val ipNum = ip2Long(ip)
    val lines = readData("ip.txt")
    val index = binarySearch(lines, ipNum)
    index
  }

  /**
   * 二进制ip转换为十进制ip
   *
   * @param ip 二进制ip
   * @return 十进制ip
   */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readData(path: String) = {
    // 输入流
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
    var s: String = null
    var flag = true
    // buffer字符串类型的数组
    val lines = new ArrayBuffer[String]()
    while (flag) {
      // 读一行数据
      s = br.readLine()
      if (s != null)
        lines += s
      else
        flag = false
    }
    lines
  }

  /**
   * 二分法查找
   *
   * @param lines
   * @param ip
   * @return
   */
  def binarySearch(lines: ArrayBuffer[String], ip: Long): String = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      // 120.55.0.0|120.55.255.255|2016870400|2016935935|亚洲|中国|浙江|杭州||阿里巴巴|330100|China|CN|120.153576|30.287459
      if ((ip >= lines(middle).split("\\|")(2).toLong) && (ip <= lines(middle).split("\\|")(3).toLong))
        return lines(middle).split("\\|")(6)
      if (ip < lines(middle).split("\\|")(2).toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    "未知"
  }

}
