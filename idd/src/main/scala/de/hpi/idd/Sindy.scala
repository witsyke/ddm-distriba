package de.hpi.idd

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    for()
      val df = spark.read.format("csv").option("header", "true").load("csvfile.csv")

  }
}
