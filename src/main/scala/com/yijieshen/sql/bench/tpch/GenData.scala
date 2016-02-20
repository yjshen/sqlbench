package com.yijieshen.sql.bench.tpch

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

case class GenConfig(
  dbgenDir: String = null,
  scaleFactor: Int = 1,
  location: String = null,
  format: String = "orc",
  overwrite: Boolean = false,
  clusterByPartitionColumns: Boolean = false,
  filterOutNullPartitionValues: Boolean = false)

object GenData {
  def main(args: Array[String]) {
    val parser = new OptionParser[GenConfig](GenData.getClass.getCanonicalName.stripSuffix("$")) {
      head("Generate TPC-H Data using dbgen")
      opt[String]('t', "tool")
        .action((x, c) => c.copy(dbgenDir = x))
        .text("local dir where dbgen tool located")
        .required()
      opt[Int]('s', "sf")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("set Scale Factor to <n>, default is 1")
      opt[String]('p', "path")
        .action((x, c) => c.copy(location = x))
        .text("HDFS path to store the generated data")
        .required()
      opt[String]('f', "format")
        .action((x, c) => c.copy(format = x))
        .text("format of the generated data, default is orc")
      opt[Boolean]('o', "overwrite")
        .action((x, c) => c.copy(overwrite = x))
        .text("overwrite if data already exist, default is false")
      opt[Boolean]('c', "cluster")
        .action((x, c) => c.copy(clusterByPartitionColumns = x))
        .text("cluster by partition columns, default is false")
      opt[Boolean]('n', "filterNull")
        .action((x, c) => c.copy(filterOutNullPartitionValues = x))
        .text("filter out null partition values, default is false")
    }

    parser.parse(args, GenConfig()) match {
      case Some(config) =>
        gen(config)
      case None =>
        System.exit(1)
    }
  }

  def gen(config: GenConfig): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName)
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)

    val tables = new Tables(sqlContext)
    tables.genData(config.dbgenDir, config.scaleFactor, config.location, config.format,
      config.overwrite, true, false, config.clusterByPartitionColumns,
      config.filterOutNullPartitionValues)
  }
}
