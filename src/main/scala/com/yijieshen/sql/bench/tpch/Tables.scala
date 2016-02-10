package com.yijieshen.sql.bench.tpch

import scala.sys.process._

import org.apache.spark.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.sql.types._

/**
  * Adapted From com.databricks.spark.sql.perf.tpcds.Tables
 *
  * @param sqlContext
  * @param dbgenDir the dbgen tool's dir, should exist on each worker
  * @param scaleFactor
  */
class Tables(sqlContext: SQLContext, dbgenDir: String, scaleFactor: Int) extends Serializable with Logging {
  import sqlContext.implicits._

  def sparkContext = sqlContext.sparkContext
  val dbgen = s"$dbgenDir/dbgen"

  case class Table(name: String, nameAlias: String, partitionColumns: Seq[String], fields: StructField*) {
    val schema = StructType(fields)
    val partitions = if (partitionColumns.isEmpty) 1 else 100

    def nonPartitioned: Table = {
      Table(name, nameAlias, Nil, fields : _*)
    }

    /**
      *  If convertToSchema is true, the data from generator will be parsed into columns and
      *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
      */
    def df(convertToSchema: Boolean) = {
      val generatedData = {
        sparkContext.parallelize(1 to partitions, partitions).flatMap { i =>
          val localToolsDir = if (new java.io.File(dbgen).exists) {
            dbgenDir
          } else if (new java.io.File(s"/$dbgen").exists) {
            s"/$dbgenDir"
          } else {
            sys.error(s"Could not find dbgen at $dbgen or /$dbgen. Run install")
          }

          val parallel = if (partitions > 1) s"-C $partitions -S $i" else ""
          val fileName = if (partitions > 1) s"$name.tbl.$i" else ""
          val commands = Seq(
            "bash", "-c",
            s"cd $localToolsDir && ./dbgen -f -T $nameAlias -s $scaleFactor $parallel && cat $fileName")
          println(commands)
          commands.lines
        }
      }

      generatedData.setName(s"$name, sf=$scaleFactor, strings")

      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            col(f.name).cast(f.dataType).as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def useDoubleForDecimal(): Table = {
      val newFields = fields.map { field =>
        val newDataType = field.dataType match {
          case decimal: DecimalType => DoubleType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, nameAlias, partitionColumns, newFields:_*)
    }

    def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = df(format != "text")
      val tempTableName = s"${name}_text"
      data.registerTempTable(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields.map { field =>
            field.name
          }.mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
               |SELECT
               |  $columnString
               |FROM
               |  $tempTableName
               |$predicates
               |DISTRIBUTE BY
               |  $partitionColumnString
            """.stripMargin
          val grouped = sqlContext.sql(query)
          println(s"Pre-clustering with partitioning columns with query $query.")
          logInfo(s"Pre-clustering with partitioning columns with query $query.")
          grouped.write
        } else {
          data.write
        }
      } else {
        // If the table is not partitioned, coalesce the data to a single file.
        data.coalesce(1).write
      }
      writer.format(format).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns : _*)
      }
      println(s"Generating table $name in database to $location with save mode $mode.")
      logInfo(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
      sqlContext.dropTempTable(tempTableName)
    }

    def createTemporaryTable(location: String, format: String): Unit = {
      println(s"Creating temporary table $name using data stored in $location.")
      logInfo(s"Creating temporary table $name using data stored in $location.")
      sqlContext.read.format(format).load(location).registerTempTable(name)
    }
  }

  def genData(
    location: String,
    format: String,
    overwrite: Boolean,
    partitionTables: Boolean,
    useDoubleForDecimal: Boolean,
    clusterByPartitionColumns: Boolean,
    filterOutNullPartitionValues: Boolean,
    tableFilter: String = ""): Unit = {
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (!tableFilter.isEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    val withSpecifiedDataType = if (useDoubleForDecimal) {
      tablesToBeGenerated.map(_.useDoubleForDecimal())
    } else {
      tablesToBeGenerated
    }

    withSpecifiedDataType.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues)
    }
  }

  def createTemporaryTables(location: String, format: String, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createTemporaryTable(tableLocation, format)
    }
  }

  // TODO distributed by which col?
  // TODO date type instead of string?
  val tables = Seq(
    Table("lineitem", "L",
      partitionColumns = "l_orderkey" :: Nil,
      'l_orderkey             .long,
      'l_partkey              .int,
      'l_suppkey              .int,
      'l_linenumber           .int,
      'l_quantity             .double,
      'l_extendedprice        .double,
      'l_discount             .double,
      'l_tax                  .double,
      'l_returnflag           .string,
      'l_linestatus           .string,
      'l_shipdate             .string,
      'l_commitdate           .string,
      'l_receiptdate          .string,
      'l_shipinstruct         .string,
      'l_shipmode             .string,
      'l_comment              .string),
    Table("orders", "O",
      partitionColumns = "o_orderkey" :: Nil,
      'o_orderkey             .long,
      'o_custkey              .int,
      'o_orderstatus          .string,
      'o_totalprice           .double,
      'o_orderdate            .string,
      'o_orderpriority        .string,
      'o_clerk                .string,
      'o_shippriority         .int,
      'o_comment              .string),
    Table("partsupp", "S",
      partitionColumns = "ps_partkey" :: "ps_suppkey" :: Nil,
      'ps_partkey             .int,
      'ps_suppkey             .int,
      'ps_availqty            .int,
      'ps_supplycost          .double,
      'ps_comment             .string),
    Table("customer", "c",
      partitionColumns = "c_custkey" :: Nil,
      'c_custkey              .int,
      'c_name                 .string,
      'c_address              .string,
      'c_nationkey            .int,
      'c_phone                .string,
      'c_acctbal              .double,
      'c_mktsegment           .string,
      'c_comment              .string),
    Table("part", "P",
      partitionColumns = "p_partkey" :: Nil,
      'p_partkey              .int,
      'p_name                 .string,
      'p_mfgr                 .string,
      'p_brand                .string,
      'p_type                 .string,
      'p_size                 .int,
      'p_container            .string,
      'p_retailprice          .double,
      'p_comment              .string),
    Table("supplier", "s",
      partitionColumns = "supplier" :: Nil,
      's_suppkey              .int,
      's_name                 .string,
      's_address              .string,
      's_nationkey            .int,
      's_phone                .string,
      's_acctbal              .double,
      's_comment              .string),
    Table("nation", "n",
      partitionColumns = Nil,
      'n_nationkey            .int,
      'n_name                 .string,
      'n_regionkey            .int,
      'n_comment              .string),
    Table("region", "r",
      partitionColumns = Nil,
      'r_regionkey            .int,
      'r_name                 .string,
      'r_comment              .string)
  )
}
