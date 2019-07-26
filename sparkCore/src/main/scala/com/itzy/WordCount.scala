package com.itzy

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: ZY
  * @Date: 2019/6/11 17:37
  * @Version 1.0
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf并设置app名称
    val conf = new SparkConf().setAppName("wc")

    //创建sc,该对象是spark App的入口
    val sc = new SparkContext(conf)

    //使用sc创建RDD并执行
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))

    //关闭连接
    sc.stop()
  }

}
