package com.itzy

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: ZY
  * @Date: 2019/6/14 14:23
  * @Version 1.0
  */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    //首先创建conf
    val conf = new SparkConf().setMaster("local[*]").setAppName("HelloWorld")

    //获取上下文
    //val sc = new SparkContext(conf)

    //val session = new SparkSession(sc)

    //获取SparkSession
    val ss = SparkSession.builder().config(conf).getOrCreate()

    val frame = ss.read.json("C:\\Users\\ZYYY\\Desktop\\111.json")

    frame.show()

    frame.createOrReplaceTempView("people")

    val sql = ss.sql("select * from people")

    val sql1 = ss.sql("select * from people where age >222")

    sql.show()

    sql1.show()
  }

}
