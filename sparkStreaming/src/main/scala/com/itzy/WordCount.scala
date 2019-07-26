package com.itzy

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: ZY
  * @Date: 2019/6/15 16:42
  * @Version 1.0
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    //创建conf
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")

    //创建SparkStreamingContext
    val ssc = new StreamingContext(conf,Seconds(10))

    //通过监控端口创建 DStream，读进来的数据为一行行
    val lineStreams = ssc.socketTextStream("zhangyu111",9999)

    //将每一行数据做切分，形成一个个单词
    val wordStreams = lineStreams.flatMap(_.split(" "))

    //将单词映射成元组（word,1）
    val map = wordStreams.map((_,1))

    //将相同的单词次数做统计
    val unit = map.reduceByKey(_+_)

    //打印
    unit.print()

    //启动 SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }

}
