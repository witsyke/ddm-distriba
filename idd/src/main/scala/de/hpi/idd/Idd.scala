package de.hpi.idd

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.File

import com.beust.jcommander.JCommander


object Idd extends App {


  override def main(args: Array[String]): Unit = {

    val DEFAULT_PATH = "./TPCH/"
    val DEFAULT_CORES = 4 // max number of cores
    val DEFAULT_PARTITIONS = 200 // default number of shuffle partitions

    val command = new Command()
    val jc = new JCommander()
    jc.addObject(command)

    jc.parse(args: _*)

    val path = if (command.input_path == null) DEFAULT_PATH else command.input_path
    val cores = if (command.input_cores == 0) DEFAULT_CORES else command.input_cores
    val partitions = if (command.input_partitions == 0) DEFAULT_PARTITIONS else command.input_partitions

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
    println("local[" + cores + "]")

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------
    val sparkBuilder = SparkSession
      .builder()
      .appName("Idd")
      .master("local[" + cores + "]") // TODO what should we do here for the cluster deployment
    val spark = sparkBuilder.getOrCreate()

    spark.conf.set("spark.executor.cores", cores)
    spark.conf.set("spark.sql.shuffle.partitions", partitions)

    //------------------------------------------------------------------------------------------------------------------
    // Reading the files
    //------------------------------------------------------------------------------------------------------------------
    val inputs = new File(path)
      .listFiles
      .filter(_.isFile)
      .map(_.getPath).toList

    //------------------------------------------------------------------------------------------------------------------
    // Execute IND-Discovery
    //------------------------------------------------------------------------------------------------------------------
    time {
      Sindy.discoverINDs(inputs, spark)
    }
  }
}
