package com.yijieshen.sql.bench

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

abstract class Benchmark(@transient val sqlContext: SQLContext)
  extends Serializable {

  import sqlContext.implicits._

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))

  val resultsLocation =
    sqlContext.getAllConfs.getOrElse(
      "spark.sql.perf.results",
      "hdfs://localhost:9000/spark/sql/performance")

  protected def sparkContext = sqlContext.sparkContext

  val buildInfo = Map.empty[String, String]

  def currentConfiguration = BenchmarkConfiguration(
    sqlConf = sqlContext.getAllConfs,
    sparkConf = sparkContext.getConf.getAll.toMap,
    defaultParallelism = sparkContext.defaultParallelism,
    buildInfo = buildInfo)

  /**
    * A Variation represents a setting (e.g. the number of shuffle partitions or if tables
    * are cached in memory) that we want to change in a experiment run.
    * A Variation has three parts, `name`, `options`, and `setup`.
    * The `name` is the identifier of a Variation. `options` is a Seq of options that
    * will be used for a query. Basically, a query will be executed with every option
    * defined in the list of `options`. `setup` defines the needed action for every
    * option. For example, the following Variation is used to change the number of shuffle
    * partitions of a query. The name of the Variation is "shufflePartitions". There are
    * two options, 200 and 2000. The setup is used to set the value of property
    * "spark.sql.shuffle.partitions".
    *
    * {{{
    *   Variation("shufflePartitions", Seq("200", "2000")) {
    *     case num => sqlContext.setConf("spark.sql.shuffle.partitions", num)
    *   }
    * }}}
    */
  case class Variation[T](name: String, options: Seq[T])(val setup: T => Unit)

  /**
    * Starts an experiment run with a given set of executions to run.
    *
    * @param executionsToRun a list of executions to run.
    * @param includeBreakdown If it is true, breakdown results of an execution will be recorded.
    *                         Setting it to true may significantly increase the time used to
    *                         run an execution.
    * @param iterations The number of iterations to run of each execution.
    * @param variations [[Variation]]s used in this run.  The cross product of all variations will be
    *                   run for each execution * iteration.
    * @param tags Tags of this run.
    * @param timeout wait at most timeout milliseconds for each query, 0 means wait forever
    * @return It returns a ExperimentStatus object that can be used to
    *         track the progress of this experiment run.
    */
  def runExperiment(
    executionsToRun: Seq[Benchmarkable],
    includeBreakdown: Boolean = false,
    iterations: Int = 3,
    variations: Seq[Variation[_]] = Seq(Variation("StandardRun", Seq("true")) { _ => {} }),
    tags: Map[String, String] = Map.empty,
    timeout: Long = 0L) = {

    class ExperimentStatus {
      val currentResults = new collection.mutable.ArrayBuffer[BenchmarkResult]()
      val currentRuns = new collection.mutable.ArrayBuffer[ExperimentRun]()
      val currentMessages = new collection.mutable.ArrayBuffer[String]()

      def logMessage(msg: String) = {
        println(msg)
        currentMessages += msg
      }

      @volatile var currentConfig = ""
      @volatile var failures = 0
      @volatile var startTime = 0L

      /** An optional log collection task that will run after the experiment. */
      @volatile var logCollection: () => Unit = () => {}


      def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
        case Nil => List(Nil)
        case h :: t => for(xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
      }

      val timestamp = System.currentTimeMillis()
      val resultPath = s"$resultsLocation/timestamp=$timestamp"
      val combinations = cartesianProduct(variations.map(l => (0 until l.options.size).toList).toList)
      val resultsFuture = Future {

        // If we're running queries, create tables for them
        executionsToRun
          .collect { case query: Query => query }
          .flatMap { query =>
            try {
              query.newDataFrame().queryExecution.logical.collect {
                case UnresolvedRelation(t, _) => t.table
              }
            } catch {
              // ignore the queries that can't be parsed
              case e: Exception => Seq()
            }
          }
          .distinct
          .foreach { name =>
            try {
              sqlContext.table(name)
              logMessage(s"Table $name exists.")
            } catch {
              case ae: Exception =>
                // the table could be subquery
                logMessage(s"Couldn't read table $name and its not defined as a Benchmark.Table.")
            }
          }

        // Run the benchmarks!
        val results = (1 to iterations).flatMap { i =>
          combinations.map { setup =>
            val currentOptions = variations.asInstanceOf[Seq[Variation[Any]]].zip(setup).map {
              case (v, idx) =>
                v.setup(v.options(idx))
                v.name -> v.options(idx).toString
            }
            currentConfig = currentOptions.map { case (k,v) => s"$k: $v" }.mkString(", ")

            val result = ExperimentRun(
              timestamp = timestamp,
              iteration = i,
              tags = currentOptions.toMap ++ tags,
              configuration = currentConfiguration,

              executionsToRun.flatMap { q =>
                val setup = s"iteration: $i, ${currentOptions.map { case (k, v) => s"$k=$v"}.mkString(", ")}"
                logMessage(s"Running execution ${q.name} $setup")

                startTime = System.currentTimeMillis()

                val singleResult =
                  q.benchmark(includeBreakdown, setup, currentMessages, timeout)

                singleResult.failure.foreach { f =>
                  failures += 1
                  logMessage(s"Execution '${q.name}' failed: ${f.message}")
                }
                singleResult.executionTime.foreach { time =>
                  logMessage(s"Execution time: ${time / 1000}s")
                }
                currentResults += singleResult
                singleResult :: Nil
              })

            currentRuns += result

            result
          }
        }

        try {
          logMessage(s"Results written to table: 'sqlPerformance' at $resultPath")
          results.toDF()
            .coalesce(1)
            .write
            .format("json")
            .save(resultPath)

          results.toDF()
        } catch {
          case e: Throwable => logMessage(s"Failed to write data: $e")
        }

        logCollection()
      }

      /** Waits for the finish of the experiment. */
      def waitForFinish(timeoutInHours: Int) = {
        Await.result(resultsFuture, timeoutInHours.hours)
      }

      /** Returns results from an actively running experiment. */
      def getCurrentResults() = {
        val tbl = sqlContext.createDataFrame(currentResults)
        tbl.registerTempTable("currentResults")
        tbl
      }

      /** Returns full iterations from an actively running experiment. */
      def getCurrentRuns() = {
        val tbl = sqlContext.createDataFrame(currentRuns)
        tbl.registerTempTable("currentRuns")
        tbl
      }

      def tail(n: Int = 20) = {
        currentMessages.takeRight(n).mkString("\n")
      }

      def status =
        if (resultsFuture.isCompleted) {
          if (resultsFuture.value.get.isFailure) "Failed" else "Successful"
        } else {
          "Running"
        }
    }
    new ExperimentStatus
  }

}
