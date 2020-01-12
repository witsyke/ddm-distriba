package de.hpi.idd

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.explode

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // List (filenames)
    //  .map(build URL)
    //  .map(read file)
    //  .explode(have to check the params for this fucntion)
    //  .flatMap (I -> I.split(every element) //returns long list with all elements, like union
    //  .collect_set(check parameters)
    // flatmap(X-> X.split().map(r->r, x\r) unsure if x is abiliable here, bht there should be an option to access it > x\r = x.remove(r) //set should have a remove fucntion

    //one last reduction step should be missing

    inputs.foreach( filename => {
      iddPipeline(filename)

    })

    def iddPipeline(filename: String): Unit = {
      val tableData = spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(filename)
      val expTableData = tableData.withColumn("*", explode($"*"))


    }

  }
}
