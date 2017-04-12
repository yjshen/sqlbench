package com.yijieshen.sql.bench

import org.apache.spark.sql.execution.SparkPlan

import scala.sys.process._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import scopt.OptionParser

import scala.util.Random

case class AggConfig(end: Long = 0, p: Int = 0, gcn: Int = 0, card: Int= 100, cn: Int = 0, mode: String = "EXEC")

object AggBench {
  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[AggConfig](AggBench.getClass.getCanonicalName.stripSuffix("$")) {
      head("Agg Operator Benchmark")
      opt[Long]('e', "end")
        .action((x, c) => c.copy(end = x))
        .text("num of items to aggregate")
      opt[Int]('p', "partition")
        .action((x, c) => c.copy(p = x))
        .text("num of tasks to agg")
      opt[Int]('g', "grpColumnNum")
        .action((x, c) => c.copy(gcn = x))
        .text("num of group by columns")
      opt[Int]('c', "cardinality")
        .action((x, c) => c.copy(card = x))
        .text("cardinality of each group by column")
      opt[Int]('n', "columnNum")
        .action((x, c) => c.copy(cn = x))
        .text("num of agg sum columns")
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
    parser.parse(args, AggConfig()) match {
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

  def run(config: AggConfig): Unit = {
    val conf = new SparkConf().setAppName("AggBench")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val grpColumns = (0 until config.gcn).map(i => randInt(Random.nextInt(), config.card).as(s"g_$i"))
    val grpColumnNames = (0 until config.gcn).map(i => s"g_$i")
    val aggColumns = (0 until config.cn).map(i => ($"id" + i).as(s"o_$i"))
    val aggs = (0 until config.cn).map(i => (s"o_$i", "sum")).toMap

    val result = sqlContext.range(0, config.end, 1, config.p)
      .select(grpColumns ++ aggColumns :_*)
      .groupBy(grpColumnNames.head, grpColumnNames.tail :_*)
      .agg(aggs)
    val qe = result.queryExecution
    val depth = qe.executedPlan.collect { case p: SparkPlan => p }.size
    val physicalOperators = (0 until depth).map(i => qe.executedPlan(i))
    val plan = physicalOperators(2)


    if (config.mode.equals("EXP")) {
      println(plan)
    } else {
      if (new java.io.File("/home/syj/free_memory.sh").exists) {
        val commands = Seq("bash", "-c", s"/home/syj/free_memory.sh")
        commands.!!
        System.err.println("free_memory succeed")
      } else {
        System.err.println("free_memory script doesn't exists")
      }

      val time = measureTimeMs {
        plan.execute().foreach((row: Any) => Unit)
      }

      println(s"Agg takes ${time / 1000}s to finish.")
    }
    sc.stop()
  }
}
