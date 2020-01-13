package de.hpi.idd

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io.File


object Idd extends App {


  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //------------------------------------------------------------------------------------------------------------------
    // Time mesaurement
    //------------------------------------------------------------------------------------------------------------------

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }
    // TODO add jcommander to parse commandline arguments -> look at how this was done for akka exercises
    // TODO clean up code so no useless commented code is left
    //if (args.length == 0 | args.length> 2) {
    //  println("No args or the wrong number of args given")
    //}

    var TPCH_path = "data/TPCH/"
    var cores = 4
    //    TPCH_path = args(0)
    //    cores = args(1).toInt

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------


    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    // .enableHiveSupport()
    val spark = sparkBuilder.getOrCreate()



    //Set max number of cores
    spark.conf.set("spark.executor.cores", cores)

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    //------------------------------------------------------------------------------------------------------------------
    // Reading the files
    //------------------------------------------------------------------------------------------------------------------

    val inputs = new File(TPCH_path)
      .listFiles
      .filter(_.isFile)
      .map(_.getPath).toList

    //val inputs = List("nation", "region", "supplier", "customer", "part", "lineitem", "orders")
    //  .map(name => s"data/TPCH/tpch_$name.csv")

    //------------------------------------------------------------------------------------------------------------------
    // Execute IND-Discovery
    //------------------------------------------------------------------------------------------------------------------

    time {
      Sindy.discoverINDs(inputs, spark)
    }
  }
}
