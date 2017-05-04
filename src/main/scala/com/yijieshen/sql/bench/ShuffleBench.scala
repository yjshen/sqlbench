package com.yijieshen.sql.bench

import scala.util.Random
import scala.sys.process._

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

case class ShuffleConfig(end: Long = 0, p: Int = 0, tpe: String = "LONG", ci: Int = 0, cl: Long = 0, op: Int = 0, mode: String = "EXEC")

object ShuffleBench {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[ShuffleConfig](ShuffleBench.getClass.getCanonicalName.stripSuffix("$")) {
      head("Shuffle Operator Benchmark")
      opt[Long]('e', "end")
        .action((x, c) => c.copy(end = x))
        .text("num of items to shuffle")
      opt[Int]('p', "partition")
        .action((x, c) => c.copy(p = x))
        .text("num of tasks to shuffle")
      opt[String]('t', "type")
        .action((x, c) => c.copy(tpe = x))
        .text("type of shuffle column")
      opt[Int]("ci")
        .action((x, c) => c.copy(ci = x))
        .text("cardinality of int column")
      opt[Long]("cl")
        .action((x, c) => c.copy(cl = x))
        .text("cardinality of long column")
      opt[Int]("op")
        .action((x, c) => c.copy(op = x))
        .text("num of output partitions")
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
    parser.parse(args, ShuffleConfig()) match {
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

  def run(config: ShuffleConfig): Unit = {
    val conf = new SparkConf().setAppName("ShuffleBench")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val c: Column = if (config.tpe.toUpperCase.equals("INT")) {
      randInt(Random.nextInt(), config.ci).as("c")
    } else if (config.tpe.toUpperCase.equals("DOUBLE")) {
      rand().as("c")
    } else { // long
      pmod($"id", lit(config.cl)).as("c")
    }

    val result = sqlContext.range(0, config.end, 1, config.p)
      .select(c)
      .repartition(config.op, $"c")

    if (config.mode.equals("EXP")) {
      result.explain(true)
    } else {
      if (new java.io.File("/home/syj/free_memory.sh").exists) {
        val commands = Seq("bash", "-c", s"/home/syj/free_memory.sh")
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
      println(s"Shuffle takes ${time / 1000}s to finish.")
    }
    sc.stop()
  }

}
