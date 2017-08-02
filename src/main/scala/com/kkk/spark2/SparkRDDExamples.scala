package com.kkk.spark2

import org.apache.spark.sql.SparkSession

/**
  * Created by lf50 on 31/07/17.
  */
object SparkRDDExamples {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TryTDD").master("local[1]").getOrCreate()

  }

}
