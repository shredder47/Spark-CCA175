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


  println("bsearch launched around 22nd aug,Weekly trended session volume for it vs gsearch nonbrand!")


  spark.sql(
    """
      |
      | SELECT
      |
      | MIN(DATE(created_at)) as start_date,
      | COUNT(ws.website_session_id) as total_sessions,
      | COUNT(CASE WHEN utm_source= 'gsearch' THEN website_session_id ELSE NULL END) as gsearch_session,
      | COUNT(CASE WHEN utm_source= 'bsearch' THEN website_session_id ELSE NULL END) as bsearch_session
      |
      | FROM website_sessions ws
      | WHERE utm_source IN ('gsearch','bsearch') AND created_at BETWEEN '2012-08-22' AND '2012-11-29' and utm_campaign='nonbrand'
      | GROUP BY date_format(created_at,'w')
      | ORDER BY start_date
      |
      |
      |
      |""".stripMargin).show(100,truncate = false)

  println("bsearch launched around 22nd aug,Compare for gsearch and bsearch , pull mobile traffic percentage")

  spark.sql(
    """
      | SELECT
      | utm_source,
      | COUNT(ws.website_session_id) as total_sessions,
      | COUNT(CASE WHEN device_type = 'mobile' THEN website_session_id ELSE NULL END)  as mobile_session,
      | (COUNT(CASE WHEN device_type = 'mobile' THEN website_session_id ELSE NULL END) / COUNT(ws.website_session_id)) * 100 as pct_mobile_traffic
      |
      | FROM website_sessions ws
      | WHERE utm_source IN ('gsearch','bsearch') AND created_at BETWEEN '2012-08-22' AND '2012-11-30' and utm_campaign='nonbrand'
      | GROUP BY utm_source
      |
      |""".stripMargin).show(100,truncate = false)


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

  println("Weekly Session Volume for gsearch and bsearch nonbrand, by device type since November 4th to 22nd dec.")

  spark.sql(
    """
      |
      |SELECT
      |
      |MIN(DATE(created_at)) as start_date,
      |COUNT( CASE WHEN utm_source = 'bsearch' AND device_type = 'desktop' THEN website_session_id ELSE NULL END) as desktop_bsearch_sessions,
      |COUNT( CASE WHEN utm_source = 'bsearch' AND device_type = 'mobile' THEN website_session_id ELSE NULL END) as mobile_bsearch_sessions,
      |COUNT( CASE WHEN utm_source = 'gsearch' AND device_type = 'desktop' THEN website_session_id ELSE NULL END) as desktop_gsearch_sessions,
      |COUNT( CASE WHEN utm_source = 'gsearch' AND device_type = 'mobile' THEN website_session_id ELSE NULL END) as mobile_gsearch_sessions,
      |
      |COUNT( CASE WHEN utm_source = 'bsearch' AND device_type = 'mobile' THEN website_session_id ELSE NULL END)
      |       /
      |COUNT( CASE WHEN utm_source = 'gsearch' AND device_type = 'mobile' THEN website_session_id ELSE NULL END) as pct_bsearch_to_gsearch_mobile_sessions,
      |
      |COUNT( CASE WHEN utm_source = 'bsearch' AND device_type = 'desktop' THEN website_session_id ELSE NULL END)
      |       /
      |COUNT( CASE WHEN utm_source = 'gsearch' AND device_type = 'desktop' THEN website_session_id ELSE NULL END) as pct_bsearch_to_gsearch_desktop_sessions
      |
      |FROM website_sessions
      |WHERE created_at BETWEEN '2012-11-04' AND '2012-12-22' AND utm_campaign = 'nonbrand' AND utm_source IN ('gsearch','bsearch')
      |GROUP BY date_format(created_at,'w')
      |ORDER BY start_date
      |
      |
      |""".stripMargin).show(10,truncate = false)



  println("Pull Organic Search, Direct Type-in and paid brand search sessions by month and show them as % of paid search nonbrand")


  spark.sql(
    """
      |
      | SELECT
      | MIN(DATE(created_at)) as start_date,
      | COUNT (CASE WHEN utm_campaign = 'brand' THEN website_session_id END) AS brand_session,
      | COUNT (CASE WHEN utm_campaign = 'nonbrand' THEN website_session_id END) AS nonbrand_session,
      | COUNT (CASE WHEN utm_source IS NULL AND http_referer IS NULL THEN website_session_id END) AS direct_type,
      | COUNT (CASE WHEN utm_source IS NULL AND http_referer IS NOT NULL THEN website_session_id END) AS organic_search,
      |
      |  COUNT (CASE WHEN utm_source IS NULL AND http_referer IS NULL THEN website_session_id END)
      |   /
      |  COUNT (CASE WHEN utm_campaign = 'nonbrand' THEN website_session_id END) AS direct_type_pct,
      |
      | COUNT (CASE WHEN utm_source IS NULL AND http_referer IS NOT NULL THEN website_session_id END)
      |   /
      |  COUNT (CASE WHEN utm_campaign = 'nonbrand' THEN website_session_id END) AS organic_type_pct
      |
      |
      | FROM website_sessions
      |
      | WHERE created_at < '2012-12-23'
      | GROUP BY month(created_at)
      | ORDER BY start_date
      |
      |
      |
      |""".stripMargin).show(10,truncate = false)


   /* +----------+-------------+----------------+-----------+--------------+--------------------+--------------------+
    |start_date|brand_session|nonbrand_session|direct_type|organic_search|direct_type_pct     |organic_type_pct    |
    +----------+-------------+----------------+-----------+--------------+--------------------+--------------------+
    |2012-03-19|10           |1852            |9          |8             |0.004859611231101512|0.004319654427645789|
    |2012-04-01|76           |3509            |71         |78            |0.020233684810487318|0.022228555143915644|
    |2012-05-01|140          |3295            |151        |150           |0.04582701062215478 |0.04552352048558422 |
    |2012-06-01|164          |3439            |170        |190           |0.04943297470194824 |0.055248618784530384|
    |2012-07-01|195          |3660            |187        |207           |0.051092896174863386|0.056557377049180325|
    |2012-08-01|264          |5318            |250        |265           |0.04701015419330575 |0.0498307634449041  |
    |2012-09-01|339          |5591            |285        |331           |0.05097478089787158 |0.059202289393668395|
    |2012-10-01|432          |6883            |440        |428           |0.06392561383117827 |0.06218218799941886 |
    |2012-11-01|556          |12260           |571        |624           |0.0465742251223491  |0.05089722675367047 |
    |2012-12-01|464          |6643            |482        |492           |0.07255757940689447 |0.07406292337799188 |
    +----------+-------------+----------------+-----------+--------------+--------------------+--------------------+*/


  spark.stop()

}
