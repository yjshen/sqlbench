package com.yijieshen.sql.bench

import scala.sys.process._

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import scopt.OptionParser

import scala.util.Random

case class SortConfig(end: Long = 0, p: Int = 0, scn: Int = 0, cn: Int = 0, mode: String = "EXEC")

object SortBench {
  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[SortConfig](SortBench.getClass.getCanonicalName.stripSuffix("$")) {
      head("Sort Operator Benchmark")
      opt[Long]('e', "end")
        .action((x, c) => c.copy(end = x))
        .text("num of items to sort")
      opt[Int]('p', "partition")
        .action((x, c) => c.copy(p = x))
        .text("num of tasks to sort")
      opt[Int]('s', "sortColumnNum")
        .action((x, c) => c.copy(scn = x))
        .text("num of sort by columns")
      opt[Int]('n', "columnNum")
        .action((x, c) => c.copy(cn = x))
        .text("num of non sort by columns")
      opt[String]('m', "mode")
        .action((x, c) => c.copy(mode = x.toUpperCase))
        .validate(x => if (x.toUpperCase.equals("EXEC") || x.toUpperCase.equals("EXP")) {
          success
        } else {
          failure("Option --mode must be either 'EXEC'(execution) or 'EXP'(explain)")
        })
        .text("execution mode: EXEC for execution and EXP for explain")
      help("help")
        .text("print this usage text")
    }
    parser.parse(args, SortConfig()) match {
      case Some(config) => run(config)
      case None => System.exit(1)
    }

  }

  protected def measureTimeMs[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }

  def run(config: SortConfig): Unit = {
    val conf = new SparkConf().setAppName("SortBench")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val sortColumns = (0 until config.scn).map(i => rand(Random.nextInt()).as(s"s_$i"))
    val sortColumnNames = (0 until config.scn).map(i => s"s_$i")
    val otherColumns = (0 until config.cn).map(i => ($"id" + i).as(s"o_$i"))

    val result = sqlContext.range(0, config.end, 1, config.p)
      .select(sortColumns ++ otherColumns :_*)
      .sortWithinPartitions(sortColumnNames.head, sortColumnNames.tail :_*)

    if (config.mode.equals("EXP")) {
      result.explain(true)
    } else {
      if (new java.io.File("/home/syj/free_memory.sh").exists) {
        val commands = Seq("bash", "-c", s"/home/syj/free_memory.sh")
        commands.!!
        System.err.println("free_memory succeed")
      } else if (new java.io.File("/home/netflow/free_memory.sh").exists) {
        val commands = Seq("bash", "-c", s"/home/netflow/free_memory.sh")
        commands.!!
        System.err.println("free_memory succeed")
      } else {
        System.err.println("free_memory script doesn't exists")
      }

      val time = measureTimeMs {
        val plan = result.queryExecution.executedPlan
        if (plan.outputsRowBatches) {
          plan.batchExecute().foreach(b => Unit)
        } else {
          plan.execute().foreach(r => Unit)
        }
      }
      println(s"Sort takes ${time / 1000}s to finish.")
    }
    sc.stop()
  }
}
