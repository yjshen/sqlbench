package com.yijieshen.sql.bench.tpch

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

abstract class Queries(sqlContext: SQLContext) {

  lazy val lineitem = sqlContext.table("lineitem")
  lazy val orders = sqlContext.table("orders")
  lazy val partsupp = sqlContext.table("partsupp")
  lazy val customer = sqlContext.table("customer")
  lazy val part = sqlContext.table("part")
  lazy val supplier = sqlContext.table("supplier")
  lazy val nation = sqlContext.table("nation")
  lazy val region = sqlContext.table("region")

  def result: DataFrame
}

case class Q1(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result =
    lineitem.filter('l_shipdate <= "1998-09-02")
      .groupBy('l_returnflag, 'l_linestatus)
      .agg(
        sum('l_quantity),
        sum('l_extendedprice),
        sum('l_extendedprice * (lit(1) - 'l_discount)),
        sum('l_extendedprice * (lit(1) - 'l_discount) * (lit(1) + 'l_tax)),
        avg('l_quantity),
        avg('l_extendedprice),
        avg('l_discount),
        count(lit(1)))
      .sort('l_returnflag, 'l_linestatus)
}

case class Q2(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val europe =
    region.filter('r_name === "EUROPE")
      .join(nation, 'r_regionkey === 'n_regionkey)
      .join(supplier, 'n_nationkey === 's_nationkey)
      .join(partsupp, 's_suppkey === 'ps_suppkey)

  val brass =
    part.filter('p_size === 15 && 'p_type.endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === 'p_partkey)

  val minCost =
    brass.groupBy('ps_partkey)
      .agg(min('ps_supplycost).as("min"))

  val result =
    brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort('s_acctbal.desc, 'n_name, 's_name, 'p_partkey)
      .limit(100)
}

case class Q3(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val cust = customer.filter('c_mktsegment === "BUILDING")
  val ord = orders.filter('o_orderdate < "1995-03-15")
  val li = lineitem.filter('l_shipdate > "1995-03-15")

  val result =
    cust.join(ord, 'c_custkey === 'o_custkey)
      //.select('o_orderkey, 'o_orderdate, 'o_shippriority)
      .join(li, 'o_orderkey === 'l_orderkey)
      .select('l_orderkey, ('l_extendedprice * (lit(1) - 'l_discount)).as("volume"), 'o_orderdate, 'o_shippriority)
      .groupBy('l_orderkey, 'o_orderdate, 'o_shippriority)
      .agg(sum('volume).as("revenue"))
      .sort('revenue.desc, 'o_orderdate)
      .limit(10)
}

case class Q4(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val ord = orders.filter('o_orderdate >= "1993-07-01" && 'o_orderdate < "1993-10-01")
  val li = lineitem.filter('l_commitdate < 'l_receiptdate).select('l_orderkey).distinct

  val result =
    li.join(ord, 'l_orderkey === 'o_orderkey)
      .groupBy('o_orderpriority)
      .agg(count('o_orderpriority))
      .sort('o_orderpriority)
}

case class Q5(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val ord = orders.filter('o_orderdate < "1995-01-01" && 'o_orderdate >= "1994-01-01")

  val result =
    region.filter('r_name === "ASIA")
      .join(nation, 'r_regionkey === 'n_regionkey)
      .join(supplier, 'n_nationkey === 's_nationkey)
      .join(lineitem, 's_suppkey === 'l_suppkey)
      //.select('n_name, 'l_extendedprice, 'l_discount, 'l_orderkey, 's_nationkey)
      .join(ord, 'l_orderkey === 'o_orderkey)
      .join(customer, 'o_custkey === 'c_custkey && 's_nationkey === 'c_nationkey)
      .select('n_name, ('l_extendedprice * (lit(1) - 'l_discount)).as("value"))
      .groupBy('n_name)
      .agg(sum('value).as("revenue"))
      .sort('revenue.desc)
}

case class Q6(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result =
    lineitem.filter(
      'l_shipdate >= "1994-01-01" &&
        'l_shipdate < "1995-01-01" &&
        'l_discount >= 0.05 &&
        'l_discount <= 0.07 &&
        'l_quantity < 24)
      .agg(sum('l_extendedprice * 'l_discount))
}

case class Q7(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val na = nation.filter('n_name === "FRANCE" || 'n_name === "GERMANY")
  val li = lineitem.filter('l_shipdate >= "1995-01-01" && 'l_shipdate <= "1996-12-31")

  val sp_na =
    na.join(supplier, 'n_nationkey === 's_nationkey)
      .join(li, 's_suppkey === 'l_suppkey)
      .select('n_name.as("supp_nation"), 'l_orderkey, 'l_extendedprice, 'l_discount, 'l_shipdate)

  val result =
    na.join(customer, 'n_nationkey === 'c_nationkey)
      .join(orders, 'c_custkey === 'o_custkey)
      .select('n_name.as("cust_nation"), 'o_orderkey)
      .join(sp_na, 'o_orderkey === 'l_orderkey)
      .filter(
        'supp_nation === "FRANCE" && 'cust_nation === "GERMANY" ||
          'supp_nation === "GERMANY" && 'cust_nation === "FRANCE")
      .select(
        'supp_nation,
        'cust_nation,
        substring('l_shipdate, 0, 4).as("l_year"),
        'l_extendedprice * (lit(1) - $"l_discount").as("volume"))
      .groupBy('supp_nation, 'cust_nation, 'l_year)
      .agg(sum('volume).as("revenue"))
      .sort('supp_nation, 'cust_nation, 'l_year)
}

case class Q8(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q9(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result =
    part.filter('p_name.contains("green"))
      .join(lineitem, 'p_suppkey === 'l_suppkey)
      .join(partsupp, 'l_suppkey === 'ps_suppkey && 'l_partkey === 'ps_partkey)
      .join(orders, 'o_orderkey === 'l_orderkey)
      .join(supplier, 's_suppkey === 'l_suppkey)
      .join(nation, 's_nationkey === 'n_nationkey)
      .select(
        'n_name,
        substring('o_orderdate, 0, 4).as("o_year"),
        ('l_extendedprice * (lit(1) - 'l_discount) - 'ps_supplycost * 'l_quantity).as("amount"))
      .groupBy('n_name, 'o_year)
      .agg(sum('amount))
      .sort('n_name, 'o_year.desc)
}

case class Q10(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q11(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q12(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q13(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q14(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q15(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q16(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q17(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q18(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q19(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q20(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q21(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}

case class Q22(sqlContext: SQLContext) extends Queries(sqlContext) {
  import sqlContext.implicits._

  val result = null
}
