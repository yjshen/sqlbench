package com.yijieshen.sql.bench

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.test.TestHive
import TestHive.implicits
import TestHive.sql

object Benchmark {

  val sqlContext = TestHive
  import sqlContext.implicits._

}
