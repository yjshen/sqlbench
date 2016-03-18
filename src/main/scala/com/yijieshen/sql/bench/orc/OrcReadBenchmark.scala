package com.yijieshen.sql.bench.orc

import scala.util.Try

import org.apache.spark.sql.execution.ConvertToUnsafe
import org.apache.spark.sql.execution.vector.DissembleFromRowBatch
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

object ORCReadBenchmark {

  val sc = new SparkContext(new SparkConf())
  val sqlContext = new HiveContext(sc)

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(sqlContext.getConf(key)).toOption)
    (keys, values).zipped.foreach(sqlContext.setConf)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => sqlContext.setConf(key, value)
      }
    }
  }

  def bm(): Unit = {
    import sqlContext.implicits._

    val lineitem = sqlContext.read.orc("hdfs://172.18.11.79:9010/1torc/lineitem")
//    val lineitem = sqlContext.read.orc("hdfs://localhost:9000/sqlgen/lineitem")


    val scanBenchmark = new Benchmark("Orc Reader Scan", 1000000, 2)

//    scanBenchmark.addCase("Row-based Scan") { iter =>
//      withSQLConf("spark.sql.vectorize.enabled"-> "false") {
//        val query = lineitem.select('l_shipdate, 'l_returnflag,
//          'l_linestatus, 'l_quantity, 'l_extendedprice, 'l_discount, 'l_tax)
//        val plan = query.queryExecution.executedPlan
//        plan.execute().foreachPartition { iter =>
//          val time1 = System.nanoTime()
//          iter.foreach((row: Any) => Unit)
//          val time2 = System.nanoTime()
//
//          System.err.println(s"${(time2 - time1) / 1000000.0}")
//        }
//      }
//    }

//    scanBenchmark.addCase("Vectorized Scan") { iter =>
//      withSQLConf("spark.sql.vectorize.enabled" -> "true") {
//        val query = lineitem.select('l_shipdate, 'l_returnflag,
//          'l_linestatus, 'l_quantity, 'l_extendedprice, 'l_discount, 'l_tax)
//        val plan = query.queryExecution.executedPlan
//        plan.batchExecute().foreachPartition { iter =>
//          var sum = 0L
//          val time1 = System.nanoTime()
//          iter.foreach((rb: Any) => sum += 1)
//          val time2 = System.nanoTime()
//
//          System.err.println(s"${(time2 - time1) / 1000000.0}")
//        }
//      }
//    }

    scanBenchmark.addCase("Vectorized Scan(RowBatch -> Row)") { iter =>
      withSQLConf("spark.sql.vectorize.enabled" -> "true") {
        val query = lineitem.select('l_shipdate, 'l_returnflag,
          'l_linestatus, 'l_quantity, 'l_extendedprice, 'l_discount, 'l_tax)
        val newPlan = ConvertToUnsafe(DissembleFromRowBatch(query.queryExecution.executedPlan))
        newPlan.execute().foreachPartition { iter =>
          val time1 = System.nanoTime()
          iter.foreach((row: Any) => Unit)
          val time2 = System.nanoTime()

          System.err.println(s"${(time2 - time1) / 1000000.0}")
        }
      }
    }

    scanBenchmark.run()
  }

  def main(args: Array[String]) {
//    bm()
    val s = "a"

  }
}

