package com.kkk.spark2

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by lf50 on 31/07/17.
  */
object DataFrames {

  case class structData(
                                   calendar_date: String,
                                   week_since_active_week: Integer,
                                   items: Long,
                                   value: Double,
                                   visits: Long,
                                   in_items:Long,
                                   in_value:Double,
                                   in_visits: Long
                                 )

  val schema1 = StructType(Array(
    StructField("Calendar_Date",StringType,true),
    StructField("week_number",IntegerType,true),
    StructField("item",LongType,true),
    StructField("Value",DoubleType,true),
    StructField("visit",LongType,true),
    StructField("in_item",LongType,true),
    StructField("in_value",DoubleType,true),
    StructField("in_visit",LongType,true)
  ))



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").appName("DataFrameExamples").getOrCreate()
    val sc = spark.sparkContext
    val accessKeyId = "AKIAI2GGCY2SE5TKIBJA"
    val accessSecretKey = "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"
    val bucketName = "kkks3practice"

    spark.conf.set("fs.s3n.awsAccessKeyId",accessKeyId)
    spark.conf.set("fs.s3n.awsSecretAccessKey",accessSecretKey)

/*    val test_data=sc.parallelize(Array(
      structData("2016-01-01",1,6,2.02,8,10,20.20,30),
      structData("2016-01-02",2,6,2.02,8,10,20.20,30),
      structData("2016-01-03",3,6,2.02,8,10,20.20,30),
      structData("2016-01-01",1,6,2.02,8,10,20.20,30),
      structData("2016-01-02",2,6,2.02,8,10,20.20,30),
      structData("2016-01-03",3,6,2.02,8,10,20.20,30)
    ))*/

/*    val txtData = sc.textFile("/Users/lf50/workspace/code/spark20/src/main/resources/csvdata.dat")
    .map(row => {
      row.split(",")
    }).map(value => structData(value(0),value(1).toInt,value(2).toLong,value(3).toDouble,value(4).toLong,value(5).toLong,value(6).toDouble,value(7).toLong))

    import spark.implicits._
    val df1 = test_data.toDF()*/
    //txtData.toDF().show(false)
    //df1.show(false)
    //df1.printSchema()

    //val schema = df1.schema

    //schema.foreach(println)


    val df = spark.read
      .option("inferSchema","true")
      .option("header","true")
      //.schema(schema1)
      .csv("/Users/lf50/workspace/code/spark20/src/main/resources/csvdata.dat")
      .withColumn("dateValue", col("Calendar_Date").cast(DateType))
    .drop(col("calendar_date"))
    .filter(col("item") > 0)
    .createOrReplaceTempView("mytestdata")


    spark.sql("select * from mytestdata").write.csv("s3n://" + bucketName + "/testCsv")






  /*  println("RDD Data: ")
    test_data.foreach(println)*/


    println("Data Frame Data: ")
    /*df.show(false)
    df.printSchema()
    println(df.columns.mkString(","))*/





  }

}
