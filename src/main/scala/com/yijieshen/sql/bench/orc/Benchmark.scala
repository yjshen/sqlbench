package com.yijieshen.sql.bench.orc

import java.io.{InputStream, File}

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Try

import org.apache.commons.lang3.SystemUtils
import org.apache.spark.SparkException

/**
  * Utility class to benchmark components. An example of how to use this is:
  *  val benchmark = new Benchmark("My Benchmark", valuesPerIteration)
  *   benchmark.addCase("V1")(<function>)
  *   benchmark.addCase("V2")(<function>)
  *   benchmark.run
  * This will output the average time to run each function and the rate of each function.
  *
  * The benchmark function takes one argument that is the iteration that's being run.
  *
  * If outputPerIteration is true, the timing for each run will be printed to stdout.
  */
class Benchmark(
  name: String,
  valuesPerIteration: Long,
  iters: Int = 5,
  outputPerIteration: Boolean = false) {
  val benchmarks = mutable.ArrayBuffer.empty[Benchmark.Case]

  def addCase(name: String)(f: Int => Unit): Unit = {
    benchmarks += Benchmark.Case(name, f)
  }

  /**
    * Runs the benchmark and outputs the results to stdout. This should be copied and added as
    * a comment with the benchmark. Although the results vary from machine to machine, it should
    * provide some baseline.
    */
  def run(): Unit = {
    require(benchmarks.nonEmpty)
    // scalastyle:off
    println("Running benchmark: " + name)

    val results = benchmarks.map { c =>
      println("  Running case: " + c.name)
      Benchmark.measure(valuesPerIteration, iters, outputPerIteration)(c.fn)
    }
    println

    val firstBest = results.head.bestMs
    // The results are going to be processor specific so it is useful to include that.
    println(Benchmark.getProcessorName())
    printf("%-35s %16s %12s %13s %10s\n", name + ":", "Best/Avg Time(ms)", "Rate(M/s)",
      "Per Row(ns)", "Relative")
    println("-----------------------------------------------------------------------------------" +
      "--------")
    results.zip(benchmarks).foreach { case (result, benchmark) =>
      printf("%-35s %16s %12s %13s %10s\n",
        benchmark.name,
        "%5.0f / %4.0f" format (result.bestMs, result.avgMs),
        "%10.1f" format result.bestRate,
        "%6.1f" format (1000 / result.bestRate),
        "%3.1fX" format (firstBest / result.bestMs))
    }
    println
    // scalastyle:on
  }
}

object Benchmark {
  case class Case(name: String, fn: Int => Unit)
  case class Result(avgMs: Double, bestRate: Double, bestMs: Double)

  def executeAndGetOutput(
      command: Seq[String],
      workingDir: File = new File("."),
      extraEnvironment: Map[String, String] = Map.empty,
      redirectStderr: Boolean = true): String = {
    val process = executeCommand(command, workingDir, extraEnvironment, redirectStderr)
    val output = new StringBuffer
    val threadName = "read stdout for " + command(0)
    def appendToOutput(s: String): Unit = output.append(s)
    val stdoutThread = processStreamByLine(threadName, process.getInputStream, appendToOutput)
    val exitCode = process.waitFor()
    stdoutThread.join()   // Wait for it to finish reading output
    if (exitCode != 0) {
      throw new SparkException(s"Process $command exited with code $exitCode")
    }
    output.toString
  }


  /**
    * Execute a command and return the process running the command.
    */
  def executeCommand(
    command: Seq[String],
    workingDir: File = new File("."),
    extraEnvironment: Map[String, String] = Map.empty,
    redirectStderr: Boolean = true): Process = {
    val builder = new ProcessBuilder(command: _*).directory(workingDir)
    val environment = builder.environment()
    for ((key, value) <- extraEnvironment) {
      environment.put(key, value)
    }
    val process = builder.start()
    if (redirectStderr) {
      val threadName = "redirect stderr for command " + command(0)
      def log(s: String): Unit = s.length
      processStreamByLine(threadName, process.getErrorStream, log)
    }
    process
  }


  /**
    * Return and start a daemon thread that processes the content of the input stream line by line.
    */
  def processStreamByLine(
    threadName: String,
    inputStream: InputStream,
    processLine: String => Unit): Thread = {
    val t = new Thread(threadName) {
      override def run() {
        for (line <- Source.fromInputStream(inputStream).getLines()) {
          processLine(line)
        }
      }
    }
    t.setDaemon(true)
    t.start()
    t
  }

  /**
    * This should return a user helpful processor information. Getting at this depends on the OS.
    * This should return something like "Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz"
    */
  def getProcessorName(): String = {
    if (SystemUtils.IS_OS_MAC_OSX) {
      executeAndGetOutput(Seq("/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"))
    } else if (SystemUtils.IS_OS_LINUX) {
      Try {
        val grepPath = executeAndGetOutput(Seq("which", "grep"))
        executeAndGetOutput(Seq(grepPath, "-m", "1", "model name", "/proc/cpuinfo"))
      }.getOrElse("Unknown processor")
    } else {
      System.getenv("PROCESSOR_IDENTIFIER")
    }
  }

  /**
    * Runs a single function `f` for iters, returning the average time the function took and
    * the rate of the function.
    */
  def measure(num: Long, iters: Int, outputPerIteration: Boolean)(f: Int => Unit): Result = {
    val runTimes = ArrayBuffer[Long]()
    for (i <- 0 until iters + 1) {
      val start = System.nanoTime()

      f(i)

      val end = System.nanoTime()
      val runTime = end - start
      if (i > 0) {
        runTimes += runTime
      }

      if (outputPerIteration) {
        // scalastyle:off
        println(s"Iteration $i took ${runTime / 1000} microseconds")
        // scalastyle:on
      }
    }
    val best = runTimes.min
    val avg = runTimes.sum / iters
    Result(avg / 1000000.0, num / (best / 1000.0), best / 1000000.0)
  }
}

