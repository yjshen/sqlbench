package com.yijieshen.sql.bench

import com.yijieshen.sql.bench.tpch.{TPCH, Tables}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

case class RunConfig(
  queries: Option[String] = None,
  mode: String = "EXEC",
  iterations: Int = 3,
  breakdownEnabled: Boolean = false,
  dataPath: String = null,
  format: String = "orc")

/**
  * Run TPC-H Benchmark
  */
object Run {
  def main(args: Array[String]) {
    val parser = new OptionParser[RunConfig](Run.getClass.getCanonicalName.stripSuffix("$")) {
      head("Run TPC-H Benchmark")
      opt[String]('q', "queries")
        .action((x, c) => c.copy(queries = Some(x)))
        .validate { x =>
          try {
            val qs = x.split(',').map(_.toInt)
            if (qs.forall(i => i >= 1 && i <= 22)) {
              success
            } else {
              failure("query num should in [1, 22]")
            }
          } catch {
            case e: NumberFormatException => sys.error("query num should be int seperated by comma")
          }
        }
        .text("query indices to run, separated by comma")
      opt[String]('m', "mode")
        .action((x, c) => c.copy(mode = x.toUpperCase))
        .validate(x => if (x.toUpperCase.equals("EXEC") || x.toUpperCase.equals("EXP")) {
            success
          } else {
            failure("Option --mode must be either 'EXEC'(execution) or 'EXP'(explain)")
          })
        .text("execution mode: EXEC for execution and EXP for explain")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[Boolean]('b', "breakdown")
        .action((x, c) => c.copy(breakdownEnabled = x))
        .text("if breakdown execution is enabled")
      opt[String]('p', "path")
        .action((x, c) => c.copy(dataPath = x))
        .text("the path where tpc-h data located")
        .required()
      opt[String]('f', "format")
        .action((x, c) => c.copy(format = x))
        .text("the tpc-h data's format")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunConfig): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName)
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)

    val tables = new Tables(sqlContext)
    tables.createTemporaryTables(config.dataPath, config.format)

    val tpch = new TPCH(sqlContext)
    val queries = tpch.queries(config.queries.map(_.split(',').map(_.toInt)))

    if (config.mode.equals("EXP")) {
      tpch.explain(queries, true)
    } else {
      val status = tpch.runExperiment(queries, config.breakdownEnabled, config.iterations)
      status.waitForFinish(500)

      import sqlContext.implicits._

      sqlContext.setConf("spark.sql.shuffle.partitions", "1")
      status.getCurrentRuns()
        .withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"executionTime") as 'minTimeMs,
          max($"executionTime") as 'maxTimeMs,
          avg($"executionTime") as 'avgTimeMs,
          stddev($"executionTime") as 'stdDev)
        .orderBy("name")
        .show(truncate = false)
      println(s"""Results: sqlContext.read.json("${status.resultPath}")""")
    }
  }
}
