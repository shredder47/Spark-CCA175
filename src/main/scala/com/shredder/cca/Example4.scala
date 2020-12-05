package com.shredder.cca

import org.apache.spark.sql.SparkSession

object Example4 extends App {

  val spark = SparkSession
    .builder()
    .appName("CCA Practice")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.read.option("header", value = true).option("inferSchema", value = true).csv("data/orders.csv").createOrReplaceTempView("orders")
  spark.read.option("header", value = true).option("inferSchema", value = true).csv("data/order_items.csv").createOrReplaceTempView("orders_items")
  spark.read.option("header", value = true).option("inferSchema", value = true).csv("data/order_item_refunds.csv").createOrReplaceTempView("orders_items_refunds")
  spark.read.option("header", value = true).option("inferSchema", value = true).csv("data/products.csv").createOrReplaceTempView("products")
  spark.read.option("header", value = true).option("inferSchema", value = true).csv("data/website_pageviews.csv").createOrReplaceTempView("website_pageviews")
  spark.read.option("header", value = true).option("inferSchema", value = true).csv("data/website_sessions.csv").createOrReplaceTempView("website_sessions")


//  println("bsearch launched around 22nd aug,Weekly trended session volume for it vs gsearch nonbrand!")
//
//
//  spark.sql(
//    """
//      |
//      | SELECT
//      |
//      | MIN(DATE(created_at)) as start_date,
//      | COUNT(ws.website_session_id) as total_sessions,
//      | COUNT(CASE WHEN utm_source= 'gsearch' THEN website_session_id ELSE NULL END) as gsearch_session,
//      | COUNT(CASE WHEN utm_source= 'bsearch' THEN website_session_id ELSE NULL END) as bsearch_session
//      |
//      | FROM website_sessions ws
//      | WHERE utm_source IN ('gsearch','bsearch') AND created_at BETWEEN '2012-08-22' AND '2012-11-29' and utm_campaign='nonbrand'
//      | GROUP BY date_format(created_at,'w')
//      | ORDER BY start_date
//      |
//      |
//      |
//      |""".stripMargin).show(100,truncate = false)
//
//  println("bsearch launched around 22nd aug,Compare for gsearch and bsearch , pull mobile traffic percentage")
//
//  spark.sql(
//    """
//      | SELECT
//      | utm_source,
//      | COUNT(ws.website_session_id) as total_sessions,
//      | COUNT(CASE WHEN device_type = 'mobile' THEN website_session_id ELSE NULL END)  as mobile_session,
//      | (COUNT(CASE WHEN device_type = 'mobile' THEN website_session_id ELSE NULL END) / COUNT(ws.website_session_id)) * 100 as pct_mobile_traffic
//      |
//      | FROM website_sessions ws
//      | WHERE utm_source IN ('gsearch','bsearch') AND created_at BETWEEN '2012-08-22' AND '2012-11-30' and utm_campaign='nonbrand'
//      | GROUP BY utm_source
//      |
//      |""".stripMargin).show(100,truncate = false)


  println("Pull conversion rate for both gsearch,bsearch both nonbrand and slice by device type with data between 22nd aug to 18th sept")

  spark.sql(
    """
      | SELECT
      |
      | device_type,
      | utm_source,
      | COUNT(ws.website_session_id) as sessions,
      | COUNT(o.website_session_id) as orders,
      | COUNT(o.website_session_id) / COUNT(ws.website_session_id) as conversion_ratio,
      | COUNT( CASE WHEN ws.utm_source = 'gsearch' THEN ws.website_session_id ELSE NULL END) as gsearch_session,
      | COUNT( CASE WHEN ws.utm_source = 'gsearch' THEN o.website_session_id ELSE NULL END) as gsearch_order,
      | COUNT( CASE WHEN ws.utm_source = 'bsearch' THEN ws.website_session_id ELSE NULL END) as bsearch_session,
      | COUNT( CASE WHEN ws.utm_source = 'bsearch' THEN o.website_session_id ELSE NULL END) as bsearch_order
      |
      | FROM website_sessions ws
      | LEFT JOIN orders o ON o.website_session_id = ws.website_session_id
      | WHERE ws.utm_source IN ('gsearch','bsearch') AND ws.created_at BETWEEN '2012-08-22' AND '2012-09-19' and ws.utm_campaign='nonbrand'
      | GROUP BY ws.device_type, ws.utm_source
      | ORDER BY ws.device_type,ws.utm_source
      |
      |""".stripMargin).show(100,truncate = false)


  spark.stop()

}
