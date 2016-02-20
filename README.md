# SQL Benchmark Utility

This project aims at running various OLAP Benchmark (currently TPC-H only) on Apache Spark Platform,
getting the overall performance as well as operator level statistics, in order to understand the importance
of each optimization better.

The project's infrastructure mainly comes from Databricks' [spark-sql-perf](https://github.com/databricks/spark-sql-perf).

## Usage

Use `spark-submit --class c.y.s.b.t.GenData ...` to generate benchmark's data &
 `spark-submit --class c.y.s.b.Run ... ` to run the benchmark 