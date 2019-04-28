package com.nielsen.adviewer

import java.io.{FileNotFoundException, IOException}

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc


object AdFrequencyViewer {

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    log.info("Ad Frequency Viewer Started")
    if (args.length < 2) {
      log.error("Parameters need to be sent in below order <input_path> <output_path>")
      System.exit(-1)
    }


    val inputpath = args(0)
    val outputpath = args(1)

    val regex = "^[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}$"

    val sparkSession = SparkSession
      .builder()
      .appName("Ad Frequency Viewer")
      .master("local")
      .getOrCreate()


    try {


      val inputDF = sparkSession.read.option("header", "true").option("sep", "\t").csv(inputpath)
        .select("ad_id", "site_id", "site_url", "gu_id")
        .toDF()
      val cleanDF = inputDF.filter(inputDF.col("gu_id").rlike(regex)).toDF()
      cleanDF.cache()
      val freqDF = cleanDF.groupBy("ad_id", "site_id", "gu_id").count().withColumnRenamed("count", "frequency").
        filter("frequency>5").toDF("ad_id", "site_id", "gu_id", "frequency").groupBy("ad_id", "site_id", "frequency").count()
        .withColumnRenamed("count", "total_views")
        .orderBy(desc("frequency"))
        .toDF()

      freqDF.show()

      freqDF.count()

      freqDF.coalesce(1).write.mode("overwrite").option("delimiter", "\t").option("header", "true").csv(outputpath)


    }

    catch {

      case ex: FileNotFoundException => log.error(s"Error while reading input path $ex.getMessage")
      case ex: IOException => log.error(s"Failed while processing input $ex.getMessage")
      case ex: Exception => log.error(s"Unknown Exception $ex.getMessage")

    }

    finally {
      sparkSession.stop()
    }

  }

}
