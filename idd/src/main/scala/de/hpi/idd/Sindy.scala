package de.hpi.idd

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{explode, collect_set}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    inputs.map(spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(_))
      //.map(df => df.map(row => {row.getValuesMap(row.schema.fieldNames)}).select(explode($"value")))



      //.reduce(_ union _)
      //.select(collect_set("key").over(Window.partitionBy("value"))) //check output of explosion to check correct values // select only "set" column
      .foreach(_.show(100, false))// check if output is DF

   //   .flatMap(x => x(."set").split().map(r => (r, x - r)) // x is not available here, need to check how to fix this
  //  ) // not sure if this works split is not correct
  //  .reduceByKey((aggr, n) => (aggr += n)) //aggregation -> reduce based on r and combine sets (aggr is aggregated set, n is the set from each instance)
    // split into IND -> can we do this just for the print?


  }
}
