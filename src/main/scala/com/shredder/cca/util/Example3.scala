package com.shredder.cca.util

import org.apache.spark.sql.SparkSession

object Example3 extends App{

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


  println("Monthly Trends for gsearch sessions and orders!")

  spark.sql(
    """
      |
      | SELECT
      |
      | MIN(DATE(ws.created_at)) as start_date,
      | COUNT(ws.website_session_id) as sessions,
      | COUNT(o.order_id) as orders
      |
      | FROM website_sessions ws
      | LEFT JOIN orders o ON ws.website_session_id = o.website_session_id
      | WHERE ws.created_at < '2012-11-27' AND ws.utm_source='gsearch'
      | GROUP BY month(ws.created_at)
      | ORDER by start_date
      |
      |""".stripMargin).show(100,truncate = false)

  println("Monthly Trends for gsearch sessions and orders splitted by nonbrand and brand!")

  spark.sql(
    """
      |
      | SELECT
      |
      | MIN(DATE(ws.created_at)) as start_date,
      | COUNT(ws.website_session_id) as sessions,
      | COUNT(CASE WHEN ws.utm_campaign = 'nonbrand' THEN ws.website_session_id ELSE NULL END ) as non_brand_sessions,
      | COUNT(CASE WHEN ws.utm_campaign = 'nonbrand' THEN o.order_id ELSE NULL END ) as non_brand_order,
      | COUNT(CASE WHEN ws.utm_campaign = 'brand' THEN ws.website_session_id ELSE NULL END ) as brand_sessions,
      | COUNT(CASE WHEN ws.utm_campaign = 'brand' THEN o.order_id ELSE NULL END ) as brand_orders,
      | COUNT(o.order_id) as total_orders
      |
      | FROM website_sessions ws
      | LEFT JOIN orders o ON ws.website_session_id = o.website_session_id
      | WHERE ws.created_at < '2012-11-27' AND ws.utm_source='gsearch'
      | GROUP BY  month(ws.created_at)
      | ORDER by start_date
      |
      |""".stripMargin).show(100,truncate = false)



  println("For gsearch nonbrand, monthly session and order split by device type")

  spark.sql(
    """
      |
      | SELECT
      |
      | MIN(DATE(ws.created_at)) as start_date,
      | COUNT(ws.website_session_id) as sessions,
      | COUNT(CASE WHEN ws.device_type = 'desktop' THEN ws.website_session_id ELSE NULL END ) as desktop_sessions,
      | COUNT(CASE WHEN ws.device_type = 'desktop' THEN  o.order_id ELSE NULL END ) as desktop_order,
      | COUNT(CASE WHEN ws.device_type = 'mobile' THEN ws.website_session_id ELSE NULL END ) as mobile_sessions,
      | COUNT(CASE WHEN ws.device_type = 'mobile' THEN  o.order_id ELSE NULL END ) as mobile_order,
      | COUNT(o.order_id) as total_orders
      |
      | FROM website_sessions ws
      | LEFT JOIN orders o ON ws.website_session_id = o.website_session_id
      | WHERE ws.created_at < '2012-11-27' AND ws.utm_source='gsearch' AND ws.utm_campaign ='nonbrand'
      | GROUP BY  month(ws.created_at)
      | ORDER by start_date
      |
      |""".stripMargin).show(100,truncate = false)

  println("Monthly Trends for GSEARCH along with monthly trends for other channel!")
  spark.sql(
    """
      |
      | SELECT
      |
      | MIN(DATE(ws.created_at)) as start_date,
      | COUNT(ws.website_session_id) as sessions,
      | COUNT(CASE WHEN ws.utm_source = 'bsearch' THEN 1 ELSE NULL END ) as bsearch_sessions,
      | COUNT(CASE WHEN ws.utm_source = 'gsearch' THEN 1 ELSE NULL END ) as gsearch_sessions,
      | COUNT(CASE WHEN ws.utm_source IS NULL AND ws.http_referer IS NOT NULL THEN 1 ELSE NULL END ) as organic_search_sessions,
      | COUNT(CASE WHEN ws.utm_source IS NULL AND ws.http_referer IS NULL THEN 1 ELSE NULL END ) as direct_sessions,
      | COUNT(o.order_id) as orders
      |
      | FROM website_sessions ws
      | LEFT JOIN orders o ON ws.website_session_id = o.website_session_id
      | WHERE ws.created_at < '2012-11-27'
      | GROUP BY  month(ws.created_at)
      | ORDER by start_date
      |
      |""".stripMargin).show(100,truncate = false)

  println("Sales to order conversion by month for first 8 month!")
  spark.sql(
    """
      | SELECT
      |
      | MIN(DATE(ws.created_at)) as start_date,
      | COUNT(ws.website_session_id) as sessions,
      | COUNT(o.order_id) as orders,
      | COUNT(o.order_id) / COUNT(ws.website_session_id) as session_order_ratio
      | FROM website_sessions ws
      | LEFT JOIN orders o ON ws.website_session_id = o.website_session_id
      | WHERE ws.created_at < '2012-11-27'
      | GROUP BY  month(ws.created_at)
      | ORDER by start_date
      |
      |""".stripMargin).show(100,truncate = false)


  spark.stop()

}
