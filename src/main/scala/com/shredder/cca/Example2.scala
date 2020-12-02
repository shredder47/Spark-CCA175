package com.shredder.cca

import org.apache.spark.sql.SparkSession

object Example2 extends App {

  val spark = SparkSession
    .builder()
    .appName("CCA Practice")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  spark.read.option("header",value = true).csv("data/orders.csv").createOrReplaceTempView("orders")
  spark.read.option("header",value = true).csv("data/order_items.csv").createOrReplaceTempView("orders_items")
  spark.read.option("header",value = true).csv("data/order_item_refunds.csv").createOrReplaceTempView("orders_items_refunds")
  spark.read.option("header",value = true).csv("data/products.csv").createOrReplaceTempView("products")
  spark.read.option("header",value = true).csv("data/website_pageviews.csv").createOrReplaceTempView("website_pageviews")
  spark.read.option("header",value = true).csv("data/website_sessions.csv").createOrReplaceTempView("website_sessions")


  spark.sql(
    """
      |SELECT * FROM orders
      |""".stripMargin).show()

  spark.sql(
    """
      |SELECT * FROM orders_items
      |""".stripMargin).show()


  spark.sql(
    """
      |SELECT * FROM orders_items_refunds
      |""".stripMargin).show()


  spark.sql(
    """
      |SELECT * FROM website_pageviews
      |""".stripMargin).show()

  /******************************
   * DateTime Functions Example *
   ******************************/

  spark.sql(
    """
      | SELECT
      | created_at,
      | date_format(created_at,'G') as era,
      | date_format(created_at,'y') as year,
      | date_format(created_at,'M') as month, -- m -> minute
      | date_format(created_at,'d') as day,
      | date_format(created_at,'w') as week
      |
      | FROM orders
      | ORDER BY order_id
      | LIMIT 10
      |""".stripMargin).show(10,truncate = false)

  spark.sql(
    """
      | SELECT
      | created_at,
      |
      | year(created_at) as year,
      | month(created_at) as month,
      | day(created_at) as day,
      | weekofyear(created_at) as week
      |
      | FROM orders
      | ORDER BY order_id
      | LIMIT 10
      |""".stripMargin).show(10,truncate = false)


  /********************
   * PIVOTING Example *
   ********************/

  /* STEP 1 */
  spark.sql(
    """
      | SELECT
      | primary_product_id,
      | items_purchased,
      |
      | CASE
      |   WHEN items_purchased = 1 THEN order_id ELSE NULL
      | END as qty_1,
      | CASE
      |   WHEN items_purchased = 2 THEN order_id ELSE NULL
      | END as qty_2
      |
      | FROM orders
      | WHERE order_id > 30000
      |
      |""".stripMargin).show(100,truncate = false)

  /*+------------------+---------------+-----+-----+
    |primary_product_id|items_purchased|qty_1|qty_2|
    +------------------+---------------+-----+-----+
    |2                 |1              |30001|null |
    |3                 |1              |30002|null |
    |2                 |1              |30003|null |
    |2                 |2              |null |30004|
    |3                 |2              |null |30005|
    |2                 |2              |null |30006|
    |2                 |2              |null |30007|
    |1                 |1              |30008|null |
    +------------------+---------------+-----+-----+
  */

  /* STEP 2 */
  spark.sql(
    """
      | SELECT
      | primary_product_id,
      | COUNT(CASE WHEN items_purchased = 1 THEN order_id ELSE NULL END) as total_qty_1,
      | COUNT(CASE WHEN items_purchased = 2 THEN order_id ELSE NULL END) as total_qty_2
      | FROM orders
      | GROUP BY primary_product_id
      | ORDER BY primary_product_id
      |
      |""".stripMargin).show(100,truncate = false)


  /*
    Product by quality value sum
    +------------------+-----------+-----------+
    |primary_product_id|total_qty_1|total_qty_2|
    +------------------+-----------+-----------+
    |1                 |18104      |5757       |
    |2                 |3924       |879        |
    |3                 |2039       |1029       |
    |4                 |534        |47         |
    +------------------+-----------+-----------+
  */

  /**--------------------------------------------------------------------------------------------------**/

  /**----------------------------------------ASSIGNMENT QUESTIONS--------------------------------------**/

  /**--------------------------------------------------------------------------------------------------**/

  println("Session Volume volume by utm_source utm_campaign http_referer")

  spark.sql(
    """
      |SELECT utm_source,utm_campaign,http_referer,COUNT(website_session_id) as sessions
      |FROM website_sessions
      |WHERE created_at < '2012-04-12'
      |GROUP BY utm_source,utm_campaign,http_referer
      |ORDER BY COUNT(website_session_id) DESC
      |
      |""".stripMargin).show(100,truncate = false)


  println("Calculate CVR (conversion Rate) for gsearch|nonbrand , 4% is good for start !")

  spark.sql(
    """
      |
      |SELECT
      | COUNT(DISTINCT website_sessions.website_session_id) as sessions ,
      | COUNT(DISTINCT orders.website_session_id) as orders ,
      | COUNT(DISTINCT orders.website_session_id)/ COUNT(DISTINCT website_sessions.website_session_id) as session_to_order_conversion_rate
      |
      | FROM website_sessions
      | LEFT JOIN orders on website_sessions.website_session_id = orders.website_session_id
      | WHERE website_sessions.created_at < '2012-04-14' AND website_sessions.utm_source = 'gsearch' AND website_sessions.utm_campaign = 'nonbrand'
      |
      |""".stripMargin).show(100,truncate = false)

  println("Session volume Trends")

  spark.sql(
    """
      |
      | SELECT
      | YEAR(created_at) as year,
      | MONTH(created_at) as month,
      | MIN(DATE(created_at)) as start_date,
      | COUNT(DISTINCT website_session_id) as sessions
      |
      | FROM website_sessions
      |
      | GROUP BY 1,2
      | ORDER BY 1,2
      |
      |
      |""".stripMargin).show(100,truncate = false)


  println("gsearch nonbrand was bid down on 2012-04-15, see how the volumes impacted till date (2012-05-10)")

  spark.sql(
    """
      | SELECT
      |
      | MIN(DATE(created_at)) as week_start,
      | COUNT(DISTINCT website_session_id) as sessions
      |
      | FROM website_sessions
      | WHERE created_at < '2012-05-10'
      | GROUP BY date_format(created_at,'w')
      | ORDER BY 1
      |""".stripMargin).show(100,truncate = false)

  println("Conversion rates from session to ORDER by Device Type, till date (2012-05-11)")

  spark.sql(
    """
      | SELECT
      | website_sessions.device_type,
      | COUNT(DISTINCT website_sessions.website_session_id) as sessions,
      | COUNT(DISTINCT orders.website_session_id) as orders,
      | COUNT(DISTINCT orders.website_session_id)/COUNT(DISTINCT website_sessions.website_session_id) as session_to_order
      |
      | FROM website_sessions
      | LEFT JOIN orders on website_sessions.website_session_id = orders.website_session_id
      | WHERE website_sessions.created_at < '2012-05-11' AND utm_source = 'gsearch' AND utm_campaign = 'nonbrand'
      | GROUP BY website_sessions.device_type
      |
      |""".stripMargin).show(100,truncate = false)
      /* Mobile traffics ratio is low ! Therefore we  can rethinking our bidding! */

  println("Result of bid changes to Desktop (on 2012-05-19) using weekly trend analysis, till date (2012-06-09)")


  spark.sql(
    """
      | SELECT
      |
      | MIN(DATE(created_at)) AS week_start_date ,
      | COUNT( CASE WHEN device_type = 'mobile' THEN website_session_id ELSE NULL END ) as mob_sessions,
      | COUNT( CASE WHEN device_type = 'desktop' THEN website_session_id ELSE NULL END ) as dtop_sessions
      |
      | FROM website_sessions
      | WHERE
      | website_sessions.created_at BETWEEN '2012-04-15' AND '2012-06-09' AND utm_source = 'gsearch' AND utm_campaign = 'nonbrand'
      | GROUP BY weekofyear(created_at)
      | ORDER BY weekofyear(created_at)
      |
      |""".stripMargin).show(100,truncate = false)
  /* We can we better traffic now at Desktop after bid changes */







  spark.close()

}
