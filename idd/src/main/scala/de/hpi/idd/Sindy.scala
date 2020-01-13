package de.hpi.idd

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_set

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    inputs.map(spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(_))
      // FIXME  .map(df => df.map(row => {row.getValuesMap(row.schema.fieldNames)}).select(explode($"value")))
      .map(df => df.flatMap(row => row.schema.fieldNames.map(col => (row.getAs(col).toString, col))))
      .reduce(_ union _)
      .groupBy($"_1")
      .agg(collect_set($"_2").as("attr_set"))
      .flatMap(row => {
        val attrSet = row.getAs[Seq[String]]("attr_set")
        attrSet.map(element => (element, attrSet.filter(_ != element)))
      })
      .rdd
      .reduceByKey((aggr, n) => aggr.intersect(n))
      .toDF
      .filter(row => row.getAs[Seq[String]]("_2").nonEmpty)
      .orderBy("_1")
      .foreach(row => println(row.get(0) + " < " + row.getAs[Seq[String]](1).mkString(", ")))

  }
}
