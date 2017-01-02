package com.yijieshen.sql.bench

import scala.sys.process._

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.SparkPlan

class Query(
  override val name: String,
  buildDataFrame: => DataFrame,
  val description: String = "",
  val sqlText: Option[String] = None,
  override val executionMode: ExecutionMode = ExecutionMode.ForeachResults)
  extends Benchmarkable with Serializable {

  protected implicit def toOption[A](a: A): Option[A] = Option(a)

  override def toString: String = {
    try {
      s"""
         |== Query: $name ==
         |${buildDataFrame.queryExecution.analyzed}
       """.stripMargin
    } catch {
      case e: Exception =>
        s"""
           |== Query: $name ==
           | Can't be analyzed: $e
           |
           | $description
           """.stripMargin
    }
  }

  lazy val tablesInvolved = buildDataFrame.queryExecution.logical collect {
    case UnresolvedRelation(tableIdentifier, _) => {
      // We are ignoring the database name.
      tableIdentifier.table
    }
  }

  def newDataFrame() = buildDataFrame

  protected override def doBenchmark(
      includeBreakdown: Boolean,
      description: String = "",
      messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      val dataFrame = buildDataFrame
      val queryExecution = dataFrame.queryExecution
      // We are not counting the time of ScalaReflection.convertRowToScala.
      val parsingTime = measureTimeMs {
        queryExecution.logical
      }
      val analysisTime = measureTimeMs {
        queryExecution.analyzed
      }
      val optimizationTime = measureTimeMs {
        queryExecution.optimizedPlan
      }
      val planningTime = measureTimeMs {
        queryExecution.executedPlan
      }

      val breakdownResults = if (includeBreakdown) {
        val depth = queryExecution.executedPlan.collect { case p: SparkPlan => p }.size
        val physicalOperators = (0 until depth).map(i => (i, queryExecution.executedPlan(i))).filter(case (i: Int, p: SparkPlan) => p.shouldReuseRowBatch)
        val indexMap = physicalOperators.map { case (index, op) => (op, index) }.toMap
        val timeMap = new scala.collection.mutable.HashMap[Int, Double]

        physicalOperators.reverse.map {
          case (index, node) =>
            messages += s"Breakdown: ${node.simpleString}"
            val newNode = buildDataFrame.queryExecution.executedPlan(index)

            if (new java.io.File("/home/netflow/free_memory.sh").exists) {
              val commands = Seq("bash", "-c", s"/home/netflow/free_memory.sh")
              commands.!!
              System.err.println("free_memory succeed")
            } else {
              System.err.println("free_memory script doesn't exists")
            }

            val executionTime = measureTimeMs {
              if (newNode.outputsRowBatches) {
                newNode.batchExecute().foreach((batch: Any) => Unit)
              } else {
                newNode.execute().foreach((row: Any) => Unit)
              }
            }
            timeMap += ((index, executionTime))

            val childIndexes = node.children.map(indexMap)
            val childTime = childIndexes.map(timeMap).sum

            messages += s"Breakdown time: $executionTime (+${executionTime - childTime})"

            BreakdownResult(
              node.nodeName,
              node.simpleString.replaceAll("#\\d+", ""),
              index,
              childIndexes,
              executionTime,
              executionTime - childTime)
        }
      } else {
        Seq.empty[BreakdownResult]
      }

      // The executionTime for the entire query includes the time of type conversion from catalyst
      // to scala.
      // The executionTime for the entire query includes the time of type conversion
      // from catalyst to scala.
      var result: Option[Long] = None
      val executionTime = measureTimeMs {
        executionMode match {
          case ExecutionMode.CollectResults => dataFrame.rdd.collect()
          case ExecutionMode.ForeachResults => dataFrame.rdd.foreach { row => Unit }
          case ExecutionMode.WriteParquet(location) =>
            dataFrame.saveAsParquetFile(s"$location/$name.parquet")
          case ExecutionMode.HashResults =>
            val columnStr = dataFrame.schema.map(_.name).mkString(",")
            // SELECT SUM(HASH(col1, col2, ...)) FROM (benchmark query)
            val row =
              dataFrame
                .selectExpr(s"hash($columnStr) as hashValue")
                .groupBy()
                .sum("hashValue")
                .head()
            result = if (row.isNullAt(0)) None else Some(row.getLong(0))
        }
      }

      val joinTypes = dataFrame.queryExecution.executedPlan.collect {
        case k if k.nodeName contains "Join" => k.nodeName
      }

      BenchmarkResult(
        name = name,
        mode = executionMode.toString,
        joinTypes = joinTypes,
        tables = tablesInvolved,
        parsingTime = parsingTime,
        analysisTime = analysisTime,
        optimizationTime = optimizationTime,
        planningTime = planningTime,
        executionTime = executionTime,
        result = result,
        queryExecution = dataFrame.queryExecution.toString,
        breakDown = breakdownResults)
    } catch {
      case e: Exception =>
        BenchmarkResult(
          name = name,
          mode = executionMode.toString,
          failure = Failure(e.getClass.getName, e.getMessage, e.getStackTraceString))
    }
  }

  /** Change the ExecutionMode of this Query to HashResults, which is used to check the query result. */
  def checkResult: Query = {
    new Query(name, buildDataFrame, description, sqlText, ExecutionMode.HashResults)
  }
}

object Query {
  def apply(
    name: String,
    dataFrameBuilder: => DataFrame,
    description: String): Query = {
    new Query(name, dataFrameBuilder, description, None, ExecutionMode.CollectResults)
  }
}
