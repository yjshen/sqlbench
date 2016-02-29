package com.yijieshen.sql.bench.tpch

import com.yijieshen.sql.bench.Query
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

abstract class TPCHQuery(sqlContext: SQLContext) {

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

case class Q1(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  def result =
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

case class Q2(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val europe =
    region.filter('r_name === "EUROPE")
      .join(nation, 'r_regionkey === 'n_regionkey)
      .join(supplier, 'n_nationkey === 's_nationkey)
      .join(partsupp, 's_suppkey === 'ps_suppkey)

  lazy val brass =
    part.filter('p_size === 15 && 'p_type.endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === 'p_partkey)

  lazy val minCost =
    brass.groupBy('ps_partkey)
      .agg(min('ps_supplycost).as("min"))

  def result =
    brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort('s_acctbal.desc, 'n_name, 's_name, 'p_partkey)
      .limit(100)
}

case class Q3(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val cust = customer.filter('c_mktsegment === "BUILDING")
  lazy val ord = orders.filter('o_orderdate < "1995-03-15")
  lazy val li = lineitem.filter('l_shipdate > "1995-03-15")

  def result =
    cust.join(ord, 'c_custkey === 'o_custkey)
      //.select('o_orderkey, 'o_orderdate, 'o_shippriority)
      .join(li, 'o_orderkey === 'l_orderkey)
      .select('l_orderkey, ('l_extendedprice * (lit(1) - 'l_discount)).as("volume"), 'o_orderdate, 'o_shippriority)
      .groupBy('l_orderkey, 'o_orderdate, 'o_shippriority)
      .agg(sum('volume).as("revenue"))
      .sort('revenue.desc, 'o_orderdate)
      .limit(10)
}

case class Q4(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val ord = orders.filter('o_orderdate.between("1993-07-01", "1993-09-30"))
  lazy val li = lineitem.filter('l_commitdate < 'l_receiptdate).select('l_orderkey).distinct

  def result =
    li.join(ord, 'l_orderkey === 'o_orderkey)
      .groupBy('o_orderpriority)
      .agg(count('o_orderpriority))
      .sort('o_orderpriority)
}

case class Q5(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val ord = orders.filter('o_orderdate.between("1994-01-01", "1994-12-31"))

  def result =
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

case class Q6(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  def result =
    lineitem.filter(
      'l_shipdate >= "1994-01-01" &&
        'l_shipdate < "1995-01-01" &&
        'l_discount >= 0.05 &&
        'l_discount <= 0.07 &&
        'l_quantity < 24)
      .agg(sum('l_extendedprice * 'l_discount))
}

case class Q7(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val na = nation.filter('n_name === "FRANCE" || 'n_name === "GERMANY")
  lazy val li = lineitem.filter('l_shipdate.between("1995-01-01", "1996-12-31"))

  lazy val sp_na =
    na.join(supplier, 'n_nationkey === 's_nationkey)
      .join(li, 's_suppkey === 'l_suppkey)
      .select('n_name.as("supp_nation"), 'l_orderkey, 'l_extendedprice, 'l_discount, 'l_shipdate)

  def result =
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
        'l_shipdate.substr(0, 4).as("l_year"),
        ('l_extendedprice * (lit(1) - $"l_discount")).as("volume"))
      .groupBy('supp_nation, 'cust_nation, 'l_year)
      .agg(sum('volume).as("revenue"))
      .sort('supp_nation, 'cust_nation, 'l_year)
}

case class Q8(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val reg = region.filter('r_name === "AMERICA")
  lazy val ord = orders.filter('o_orderdate.between("1995-01-01", "1996-12-31"))
  lazy val pa = part.filter('p_type === "ECONOMY ANODIZED STEEL")

  lazy val supp_na = supplier.join(nation, 's_nationkey === 'n_nationkey)
  lazy val cust_na_reg = customer.join(nation, 'c_nationkey === 'n_nationkey)
    .join(reg, 'n_regionkey === 'r_regionkey)

  lazy val all_nations =
    lineitem.join(pa, 'l_partkey === 'p_partkey)
      .join(ord, 'l_orderkey === 'o_orderkey)
      .join(cust_na_reg, 'o_custkey === 'c_custkey)
      .join(supp_na, 'l_suppkey === 's_suppkey)
      .select(
        'o_orderdate.substr(0, 4).as("o_year"),
        ('l_extendedprice * (lit(1) - 'l_discount)).as("volume"),
        supp_na("n_name").as("nation"))

  def result =
    all_nations.groupBy('o_year)
      .agg(
        sum(when('nation === "BRAZIL", 'volume).otherwise(0)).as("brazil"),
        sum('volume).as("total"))
      .select('o_year, ('brazil / 'total).as("mkt_share"))
      .sort('o_year)
}

case class Q9(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  def result =
    part.filter('p_name.contains("green"))
      .join(lineitem, 'p_partkey === 'l_partkey)
      .join(partsupp, 'l_suppkey === 'ps_suppkey && 'l_partkey === 'ps_partkey)
      .join(orders, 'o_orderkey === 'l_orderkey)
      .join(supplier, 's_suppkey === 'l_suppkey)
      .join(nation, 's_nationkey === 'n_nationkey)
      .select(
        'n_name,
        'o_orderdate.substr(0, 4).as("o_year"),
        ('l_extendedprice * (lit(1) - 'l_discount) - 'ps_supplycost * 'l_quantity).as("amount"))
      .groupBy('n_name, 'o_year)
      .agg(sum('amount))
      .sort('n_name, 'o_year.desc)
}

case class Q10(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val li = lineitem.filter('l_returnflag === "R")

  def result =
    orders.filter('o_orderdate.between("1993-10-01", "1993-12-31"))
      .join(li, 'o_orderkey === 'l_orderkey)
      .join(customer, 'o_custkey === 'c_custkey)
      .join(nation, 'c_nationkey === 'n_nationkey)
      .groupBy('c_custkey, 'c_name, 'c_acctbal, 'c_phone, 'n_name, 'c_address, 'c_comment)
      .agg(sum('l_extendedprice * (lit(1) - 'l_discount)).as("revenue"))
      .sort('revenue.desc)
      .limit(20)
}

case class Q11(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val joint =
    nation.filter('n_name === "GERMANY")
      .join(supplier, 'n_nationkey === 's_nationkey)
      .join(partsupp, 's_suppkey === 'ps_suppkey)
      .select('ps_partkey, ('ps_supplycost * 'ps_availqty).as("value"))

  lazy val all = joint.groupBy('ps_partkey).agg(sum('value).as("part_value"))
  lazy val total = all.agg(sum('part_value).as("total_value"))

  def result =
    all.join(total)
      .filter('part_value > ('total_value * lit(0.0001)))
      .select('ps_partkey, 'part_value.as("value"))
      .sort('value.desc)
}

case class Q12(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  def result =
    lineitem.filter(('l_shipmode === "Mail" || 'l_shipmode === "SHIP") &&
      'l_commitdate < 'l_receiptdate &&
      'l_shipdate < 'l_commitdate &&
      'l_receiptdate.between("1994-01-01", "1994-12-31"))
      .join(orders, 'l_orderkey === 'o_orderkey)
      .groupBy('l_shipmode)
      .agg(
        sum(when(('o_orderpriority === "1-URGENT") || ('o_orderpriority === "2-HIGH"), 1).otherwise(0)).as("high_line_count"),
        sum(when(('o_orderpriority !== "1-URGENT") && ('o_orderpriority !== "2-HIGH"), 1).otherwise(0)).as("low_line_count"))
      .orderBy('l_shipmode)
}

case class Q13(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  def result =
    customer.join(orders, 'c_custkey === 'o_custkey && not('o_comment.like("%special%requests%")), "left_outer")
      .groupBy('c_custkey)
      .agg(count('o_orderkey).as("c_count"))
      .groupBy('c_count)
      .agg(count(lit(1)).as("custdist"))
      .sort('custdist.desc, 'c_count.desc)
}

case class Q14(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  def result =
    lineitem.filter('l_shipdate.between("1995-09-01", "1995-09-30"))
      .join(part, 'l_partkey === 'p_partkey)
      .agg(
        sum(when('p_type.startsWith("PROMO"), 'l_extendedprice * (lit(1) - 'l_discount)).otherwise(0.0)).as("promo"),
        sum('l_extendedprice * (lit(1) - 'l_discount)).as("all"))
      .select(lit(100) * 'promo / 'all)
}

case class Q15(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val revenue =
    lineitem.filter('l_shipdate.between("1996-01-01", "1996-03-31"))
      .groupBy('l_suppkey.as("supplier_no"))
      .agg(sum('l_extendedprice * (lit(1) - 'l_discount)).as("total_revenue"))

  def result =
    revenue.agg(max('total_revenue).as("max_total"))
      .join(revenue, 'total_revenue === 'max_total)
      .join(supplier, 'supplier_no === 's_suppkey)
      .select('s_suppkey, 's_name, 's_address, 's_phone, 'total_revenue)
      .sort('s_suppkey)
}

case class Q16(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val pa = part.filter(
    ('p_brand !== "Brand#45") &&
      not('p_type.startsWith("MEDIUM POLISHED")) &&
      'p_size.isin(3, 9, 14, 19, 23, 36, 45, 49))

  lazy val sup = supplier.filter(not('s_comment.like("%Customer%Complaints%")))

  def result =
    partsupp.join(pa, 'ps_partkey === 'p_partkey)
      .join(sup, 'ps_suppkey === 's_suppkey)
      .groupBy('p_brand, 'p_type, 'p_size)
      .agg(countDistinct('ps_suppkey).as("supplier_count"))
      .sort('supplier_count, 'p_brand, 'p_type, 'p_size)
}

case class Q17(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val li =
    part.filter('p_brand === "Brand#23" && 'p_container === "MED BOX")
      .select('p_partkey)
      .join(lineitem, 'l_partkey === 'p_partkey)
      .select('l_partkey, 'l_quantity, 'l_extendedprice)

  def result =
    li.groupBy('l_partkey).agg(avg('l_quantity).as("avg_quantity"))
      .select('l_partkey.as("key"), 'avg_quantity)
      .join(li, 'key === li("l_partkey"))
      .filter('l_quantity < (lit(0.2) * 'avg_quantity))
      .agg(sum('l_extendedprice).as("sum_all"))
      .select(('sum_all / lit(7.0)).as("avg_yearly"))
}

case class Q18(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val li = lineitem
    .groupBy('l_orderkey)
    .agg(sum('l_quantity).as("sum_quantity"))
    .filter('sum_quantity > 300)

  def result =
    li.join(orders, 'l_orderkey === 'o_orderkey)
      .join(customer, 'o_custkey === 'c_custkey)
      .groupBy('c_name, 'c_custkey, 'o_orderkey, 'o_orderdate, 'o_totalprice)
      .agg(sum('sum_quantity))
      .sort('o_totalprice.desc, 'o_orderdate)
      .limit(100)
}

case class Q19(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val li = lineitem.filter(
    'l_shipmode.isin("AIR", "AIR REG") &&
      'l_shipinstruct === "DELIVER IN PERSON" &&
      'l_quantity.between(1, 30))

  def result = part.join(li, 'p_partkey === 'l_partkey).filter(
    ('p_brand === "Brand#12" && 'p_container.isin("SM CASE", "SM BOX", "SM PACK", "SM PKG") &&
      'p_size.between(1, 5) && 'l_quantity.between(1, 11)) ||
      ('p_brand === "Brand#23" && 'p_container.isin("MED BAG", "MED BOX", "MED PKG", "MED PACK") &&
        'p_size.between(1, 10) && 'l_quantity.between(10, 20)) ||
      ('p_brand === "Brand#34" && 'p_container.isin("LG CASE", "LG BOX", "LG PACK", "LG PKG") &&
        'p_size.between(1, 15) && 'l_quantity.between(20, 30)))
    .agg(sum('l_extendedprice * (lit(1) - 'l_discount)))
}

case class Q20(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val li = lineitem
    .filter('l_shipdate.between("1994-01-01", "1994-12-31"))
    .groupBy('l_partkey, 'l_suppkey)
    .agg(sum('l_quantity).as("sumq"))
    .select('l_partkey, 'l_suppkey, ('sumq * lit(0.5)).as("half_sum"))
  lazy val pa = part.filter('p_name.startsWith("forest")).select('p_partkey).distinct()
  lazy val na = nation.filter('n_name === "CANADA")

  def result =
    partsupp.join(pa, 'p_partkey === 'ps_partkey)
    .join(li, 'ps_partkey === 'l_partkey && 'ps_suppkey === 'l_suppkey)
    .filter('ps_availqty > 'half_sum).select('ps_suppkey).distinct()
    .join(supplier, 'ps_suppkey === 's_suppkey)
    .join(na, 's_nationkey === 'n_nationkey)
    .select('s_name, 's_address)
    .sort('s_name)
}

case class Q21(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val li = lineitem.filter('l_receiptdate > 'l_commitdate)

  lazy val li1 = lineitem.groupBy('l_orderkey)
    .agg(countDistinct('l_suppkey).as("suppkey_count"), max('l_suppkey).as("suppkey_max"))
    .select('l_orderkey.as("key"), 'suppkey_count, 'suppkey_max)

  lazy val li2 = li.groupBy('l_orderkey)
    .agg(countDistinct('l_suppkey).as("suppkey_count"), max('l_suppkey).as("suppkey_max"))
    .select('l_orderkey.as("key"), 'suppkey_count, 'suppkey_max)

  lazy val ord = orders.filter('o_orderstatus === "F")

  def result = nation.filter('n_name === "SAUDI ARABIA")
    .join(supplier, 'n_nationkey === 's_nationkey)
    .join(li, 's_suppkey === 'l_suppkey)
    .join(ord, 'l_orderkey === 'o_orderkey)
    .join(li1, 'l_orderkey === 'key)
    .filter('suppkey_count > 1 || ('suppkey_count == 1 && 'l_suppkey == 'max_suppkey))
    .select('s_name, 'l_orderkey, 'l_suppkey)
    .join(li2, 'l_orderkey === 'key, "left_outer")
    .select('s_name, 'l_orderkey, 'l_suppkey, 'suppkey_count, 'suppkey_max)
    .filter('suppkey_count === 1 && 'l_suppkey === 'suppkey_max)
    .groupBy('s_name)
    .agg(count('l_suppkey).as("numwait"))
    .sort('numwait.desc, 's_name)
    .limit(100)
}

case class Q22(sqlContext: SQLContext) extends TPCHQuery(sqlContext) {
  import sqlContext.implicits._

  lazy val cust =
    customer.select('c_acctbal, 'c_custkey, 'c_phone.substr(0, 2).as("cntrycode"))
      .filter('cntrycode.isin("13", "31", "23", "29", "30", "18", "17"))

  lazy val avg_customer = cust.filter('c_acctbal > 0.0).agg(avg('c_acctbal).as("avg_acctbal"))

  def result =
    orders.select('o_custkey).distinct()
      .join(cust, 'o_custkey === 'c_custkey, "right_outer")
      .filter('o_custkey.isNull)
      .join(avg_customer)
      .filter('c_acctbal > 'avg_acctbal)
      .groupBy('cntrycode)
      .agg(count('c_acctbal), sum('c_acctbal))
      .sort('cntrycode)
}

object TPCHQuery {
  def allQueries(ctx: SQLContext): Seq[Query] = {
    val all = Q1(ctx) :: Q2(ctx) :: Q3(ctx) :: Q4(ctx) :: Q5(ctx) ::
      Q6(ctx) :: Q7(ctx) :: Q8(ctx) :: Q9(ctx) :: Q10(ctx) ::
      Q11(ctx) :: Q12(ctx) :: Q13(ctx) :: Q14(ctx) :: Q15(ctx) ::
      Q16(ctx) :: Q17(ctx) :: Q18(ctx) :: Q19(ctx) :: Q20(ctx) ::
      Q21(ctx) :: Q22(ctx) :: Nil
    all.map(q => Query(q.getClass.getSimpleName, q.result, "TPCH Query"))
  }

  def withFilter(selectedQueries: Seq[Int], sqlContext: SQLContext): Seq[Query] = {
    val selected = selectedQueries.map(i => s"Q$i").toSet
    allQueries(sqlContext).filter(q => selected.contains(q.name))
  }
}
