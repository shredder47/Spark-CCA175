package com.shredder.cca

import org.apache.spark.sql.SparkSession

object Example2 extends App {

  val spark = SparkSession
    .builder()
    .appName("CCA Practice")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  spark.read.option("header",value = true).option("inferSchema",true).csv("data/orders.csv").createOrReplaceTempView("orders")
  spark.read.option("header",value = true).option("inferSchema",true).csv("data/order_items.csv").createOrReplaceTempView("orders_items")
  spark.read.option("header",value = true).option("inferSchema",true).csv("data/order_item_refunds.csv").createOrReplaceTempView("orders_items_refunds")
  spark.read.option("header",value = true).option("inferSchema",true).csv("data/products.csv").createOrReplaceTempView("products")
  spark.read.option("header",value = true).option("inferSchema",true).csv("data/website_pageviews.csv").createOrReplaceTempView("website_pageviews")
  spark.read.option("header",value = true).option("inferSchema",true).csv("data/website_sessions.csv").createOrReplaceTempView("website_sessions")


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

  println("Total Pageviews ")
  spark.sql(
    """
      | SELECT
      | pageview_url,
      | COUNT(DISTINCT website_pageview_id) as total_hits
      | FROM website_pageviews
      | WHERE website_pageview_id < 10000
      | GROUP BY pageview_url
      | ORDER BY total_hits DESC
      |
      |""".stripMargin).show(100,truncate = false)


  println("First Page view per session ")
  spark.sql(
    """
      | SELECT
      | website_session_id,
      | MIN(website_pageview_id) as first_page_view_id
      |
      | FROM website_pageviews
      | GROUP BY website_session_id
      | ORDER BY website_session_id
      |
      |""".stripMargin)
    .createOrReplaceTempView("first_page_views")

  spark.sql(
    """
      | SELECT
      | first_page_views.website_session_id,
      | website_pageviews.pageview_url AS landing_page
      |
      | FROM first_page_views
      |
      | LEFT JOIN website_pageviews ON first_page_views.first_page_view_id = website_pageviews.website_pageview_id
      | ORDER BY 1
      |
      |""".stripMargin).show(10,truncate = false)

  println("First Page view Total Hits ")

  spark.sql(
    """
      | SELECT
      |
      | website_pageviews.pageview_url AS landing_page,
      | COUNT(first_page_views.website_session_id) as total_hits
      |
      | FROM first_page_views
      | LEFT JOIN website_pageviews ON first_page_views.first_page_view_id = website_pageviews.website_pageview_id
      | GROUP BY 1
      | ORDER BY 2 DESC
      |
      |
      |""".stripMargin).show(10,truncate = false)

  println("Most viewed pages as of (2012-06-09)")
  spark.sql(
    """
      | SELECT
      | pageview_url,
      | COUNT(DISTINCT(website_pageview_id)) as sessions
      |
      | FROM website_pageviews
      | WHERE created_at < '2012-06-09'
      | GROUP BY pageview_url
      | ORDER BY sessions DESC
      |
      |""".stripMargin).show(100,truncate = false)

  println("Most viewed pages as of (2012-06-09)")
  spark.sql(
    """
      | SELECT
      | pageview_url,
      | COUNT(DISTINCT(website_pageview_id)) as sessions
      |
      | FROM website_pageviews
      | WHERE created_at < '2012-06-09'
      | GROUP BY pageview_url
      | ORDER BY sessions DESC
      |
      |""".stripMargin).show(100,truncate = false)

      /* Most viewed pages as of (2012-06-09)
       +-------------------------+--------+
       |pageview_url             |sessions|
       +-------------------------+--------+
       |/home                    |10403   |
       |/products                |4239    |
       |/the-original-mr-fuzzy   |3037    |
       |/cart                    |1306    |
       |/shipping                |869     |
       |/billing                 |716     |
       |/thank-you-for-your-order|306     |
       +-------------------------+--------+*/

  // TODO: Trying finding performance of each page with various perf metrics.

  println("First Page view per session as of ('2012-06-12')")
  spark.sql(
    """
      | SELECT
      | website_session_id,
      | MIN(website_pageview_id) as first_page_view_id
      |
      | FROM website_pageviews
      | WHERE created_at < '2012-06-12'
      | GROUP BY website_session_id
      | ORDER BY website_session_id
      |
      |""".stripMargin)
    .createOrReplaceTempView("first_page_views")

  println("Landing page hits as of ('2012-06-12') ")
  spark.sql(
    """
      | SELECT
      |
      | website_pageviews.pageview_url AS landing_page,
      | COUNT(first_page_views.website_session_id) as sessions_hitting_this_landing_page
      |
      | FROM first_page_views
      | LEFT JOIN website_pageviews ON first_page_views.first_page_view_id = website_pageviews.website_pageview_id
      | WHERE created_at < '2012-06-12'
      | GROUP BY 1
      | ORDER BY 2 DESC
      |
      |""".stripMargin).show(10,truncate = false)


  println("Landing page performance between 2014-01-01 and 2014-02-01")

  // STEP 1 - Finding landing pageview_id
  spark.sql(
    """
      | SELECT
      |
      | website_sessions.website_session_id,
      | MIN(website_pageviews.website_pageview_id) as first_pageview_id
      |
      | FROM website_pageviews
      | INNER JOIN website_sessions ON website_sessions.website_session_id = website_pageviews.website_session_id
      | WHERE website_sessions.created_at BETWEEN '2014-01-01' AND '2014-02-01'
      | GROUP BY website_sessions.website_session_id
      | ORDER BY website_sessions.website_session_id
      |
      |
      |""".stripMargin).createOrReplaceTempView("first_page_views2")

  // STEP 2 - Finding landing pageview_id with page name
  spark.sql(
    """
      | SELECT
      |
      | first_page_views2.website_session_id,
      | first_page_views2.first_pageview_id,
      | website_pageviews.pageview_url AS landing_page
      |
      | FROM first_page_views2
      | LEFT JOIN website_pageviews ON website_pageviews.website_pageview_id = first_page_views2.first_pageview_id
      |
      | ORDER BY 1
      |
      |""".stripMargin).createOrReplaceTempView("first_page_views_with_name")

/*    +------------------+-----------------+------------+
      |website_session_id|first_pageview_id|landing_page|
      +------------------+-----------------+------------+
      |175251            |415127           |/products   |
      |175252            |415126           |/lander-2   |
      |175253            |415128           |/home       |
      |175254            |415134           |/lander-2   |
      |175255            |415136           |/home       |
      |175256            |415141           |/lander-2   |
      |175257            |415145           |/lander-3   |
      |175258            |415149           |/lander-2   |
      |175259            |415153           |/home       |
      |175260            |415154           |/lander-2   |
      +------------------+-----------------+------------+
      */

  // STEP 3 - Finding every above session's page visit count.

  spark.sql(
    """
      |
      | SELECT
      | first_page_views_with_name.website_session_id AS website_session_id,
      | COUNT(DISTINCT(website_pageview_id)) AS total_page_visits
      |
      | FROM website_pageviews
      | JOIN first_page_views_with_name ON website_pageviews.website_session_id = first_page_views_with_name.website_session_id
      | GROUP BY first_page_views_with_name.website_session_id
      | ORDER BY first_page_views_with_name.website_session_id
      |
      |
      |""".stripMargin).createOrReplaceTempView("session_to_total_page_visits")

      /* +------------------+-----------------+*/
      /* |website_session_id|total_page_visits|*/
      /* +------------------+-----------------+*/
      /* |175251            |3                |*/
      /* |175252            |3                |*/
      /* |175253            |3                |*/
      /* |175254            |6                |*/
      /* |175255            |7                |*/
      /* |175256            |1                |*/
      /* |175257            |3                |*/
      /* |175258            |2                |*/
      /* |175259            |1                |*/
      /* |175260            |1                |*/
      /* +------------------+-----------------+*/

  // STEP 4 - Now getting landing page of each sessions with the information above


  spark.sql(
    """
      |
      | SELECT
      | landing_page,
      | COUNT(CASE WHEN total_page_visits = 1 THEN 1 ELSE NULL END) AS total_bounced,
      | COUNT(1) AS total_sessions,
      | COUNT(CASE WHEN total_page_visits = 1 THEN 1 ELSE NULL END) / COUNT(1) AS bounce_ratio
      |
      |
      | FROM first_page_views_with_name
      | JOIN session_to_total_page_visits ON session_to_total_page_visits.website_session_id = first_page_views_with_name.website_session_id
      | GROUP BY landing_page
      |
      |""".stripMargin).show(10,truncate = false)


  println("Total Session on /home and the total bounce  of it , along with bounce_rate as of 2012-06-14 ")


    //STEP 1 - GETTING SESSION'S LANDING PAGE WHICH ARE /home

    spark.sql(
      """
        | SELECT
        |
        | ws.website_session_id,
        | MIN(wp.website_pageview_id) AS first_pageview_id
        |
        | FROM website_pageviews wp
        | JOIN website_sessions ws ON wp.website_session_id = ws.website_session_id
        | WHERE ws.created_at < '2012-06-14'
        | GROUP BY ws.website_session_id
        | ORDER BY ws.website_session_id
        |
        |
        |
        |""".stripMargin).createOrReplaceTempView("session_to_fpv_id")


      //STEP 2 -  GETTING all session ids landing to homepage


    spark.sql(
      """
        |
        | SELECT
        |
        | sfpv.website_session_id ,
        | pv.pageview_url as landing_page
        |
        | FROM website_pageviews pv
        | RIGHT JOIN session_to_fpv_id sfpv ON sfpv.first_pageview_id = pv.website_pageview_id
        |
        | WHERE pv.pageview_url= '/home'
        | ORDER BY sfpv.website_session_id
        |
        |
        |
        |""".stripMargin).createOrReplaceTempView("all_session_to_homepage")


    //STEP 3 - Finding only session what are bounced from homepage i.e  total_page_hits = 1


  spark.sql(
    """
      |
      | SELECT
      |
      | sth.website_session_id,
      | COUNT(wp.website_pageview_id) as total_page_hits
      |
      | FROM website_pageviews wp
      | LEFT JOIN all_session_to_homepage sth ON sth.website_session_id = wp.website_session_id
      | GROUP BY sth.website_session_id
      | HAVING total_page_hits = 1
      |
      |
      |""".stripMargin).createOrReplaceTempView("bounced_session_to_homepage")


  //STEP 4 - Finding total session to bounced session for home page

  spark.sql(
    """
      | SELECT
      | COUNT(ash.website_session_id) as sessions,
      | COUNT(bsh.website_session_id) as bounced_sessions,
      | COUNT(bsh.website_session_id) / COUNT(ash.website_session_id) as bounce_rate
      |
      | FROM all_session_to_homepage ash
      | LEFT JOIN bounced_session_to_homepage bsh on ash.website_session_id = bsh.website_session_id
      |
      |""".stripMargin).show(20,truncate = false)

  /*+--------+----------------+------------------+
    |sessions|bounced_sessions|bounce_rate       |
    +--------+----------------+------------------+
    |11048   |6538            |0.5917813178855902|
    +--------+----------------+------------------+
    */

  println("For  /home and /lander-1 find total sessions ,bounce rate of each as of 2012-07-28 ")

  //STEP 1 - Find the time when lander-1 was launched.So that we can limit the time frame for fair analysis.

  spark.sql(
    """
      |
      |SELECT
      |MIN(DATE(created_at)) as first_launch_date
      |
      |FROM website_pageviews
      |WHERE pageview_url = '/lander-1'
      |
      |""".stripMargin).show(10,truncate = false)

  /*  +-----------------+
      |first_launch_date|
      +-----------------+
      |2012-06-19       |
      +-----------------+ */


  spark.sql(
    """
      |
      | SELECT
      | wp.website_session_id as session_id,
      | MIN(wp.website_pageview_id) as f_page_view
      |
      | FROM website_pageviews wp
      | INNER JOIN website_sessions ws ON wp.website_session_id = ws.website_session_id
      | WHERE ws.created_at BETWEEN '2012-06-19' AND '2012-07-28' AND ws.utm_source='gsearch' AND ws.utm_campaign = 'nonbrand'
      | GROUP BY wp.website_session_id
      | ORDER BY wp.website_session_id
      |
      |""".stripMargin).createOrReplaceTempView("all_session_first_page_view")

  spark.sql(
    """
      |
      | SELECT
      |
      | aspv.session_id,
      | wp.pageview_url as landing_page
      |
      | FROM all_session_first_page_view aspv
      | LEFT JOIN website_pageviews wp ON wp.website_pageview_id  = aspv.f_page_view
      |
      | WHERE wp.pageview_url IN ('/home','/lander-1')
      |
      | ORDER BY aspv.session_id
      |
      |""".stripMargin).createOrReplaceTempView("all_sessions_for_landers")

  spark.sql(
    """
      |
      | SELECT
      |
      | asl.session_id,
      | asl.landing_page,
      |
      | COUNT(pv.website_pageview_id) as page_hits
      |
      | FROM all_sessions_for_landers asl
      | LEFT JOIN website_pageviews pv ON pv.website_session_id = asl.session_id
      | GROUP BY asl.session_id , asl.landing_page
      | HAVING page_hits = 1
      |
      |
      |""".stripMargin).createOrReplaceTempView("bounced_sessions_for_landers")


  spark.sql(
    """
      |
      | SELECT
      | asl.landing_page,
      | COUNT(asl.session_id) AS sessions,
      | COUNT(bsl.session_id) AS bounced_session,
      | COUNT(bsl.session_id) / COUNT(asl.session_id) as bounce_rate
      |
      | FROM all_sessions_for_landers asl
      | LEFT JOIN bounced_sessions_for_landers bsl ON asl.session_id = bsl.session_id
      | GROUP BY asl.landing_page
      |
      |""".stripMargin).show(100,truncate = false)


  println("For /home and /lander-1 find weekly trended data for paid nonbrand traffic's bounce rate since 1st june 2012 - AS of 2012-08-31 ")

  //STEP 1 - session's first landing page that are lander-1 or home with timestamp

  spark.sql(
    """
      |
      | SELECT
      | session_to_fpv.session_id,
      | session_to_fpv.page_view_ts,
      | website_pageviews.pageview_url as lander_page
      | FROM
      |
      | (
      |   SELECT
      |   wp.website_session_id AS session_id,
      |   MIN(wp.website_pageview_id) AS first_page_view,
      |   wp.created_at AS page_view_ts
      |   FROM website_pageviews wp
      |   LEFT JOIN website_sessions ws on wp.website_session_id = ws.website_session_id
      |   WHERE wp.created_at BETWEEN '2012-06-01' AND '2012-08-31' AND ws.utm_source='gsearch' AND ws.utm_campaign = 'nonbrand' AND wp.pageview_url IN ('/home','/lander-1')
      |   GROUP BY wp.website_session_id,wp.created_at
      | ) AS session_to_fpv
      |
      | INNER JOIN website_pageviews ON session_to_fpv.first_page_view = website_pageviews.website_pageview_id
      |
      |""".stripMargin).createOrReplaceTempView("session_to_lander_over_time")


  //STEP 2- Sessions that are bounced.

  spark.sql(
    """
      |
      | SELECT
      |
      | wp.website_session_id as session_id,
      | COUNT(wp.website_pageview_id) as total_page_views
      |
      | FROM session_to_lander_over_time slt
      | INNER JOIN website_pageviews wp ON slt.session_id = wp.website_session_id
      | GROUP BY wp.website_session_id
      | HAVING total_page_views = 1
      |
      |""".stripMargin).createOrReplaceTempView("sessions_bounced")

    // STEP 3- GET required info from session_to_lander_over_time and sessions_bounced

  spark.sql(
    """
      |
      | SELECT
      |
      | MIN(DATE(page_view_ts)) as start_date,
      | COUNT(sb.session_id) / COUNT(slt.session_id) AS bounced_sessions,
      | COUNT(CASE WHEN slt.lander_page = '/home' THEN 1 ELSE NULL END) AS home_sessions,
      | COUNT(CASE WHEN slt.lander_page = '/lander-1' THEN 1 ELSE NULL END) AS lander_sessions
      |
      | FROM session_to_lander_over_time slt
      | LEFT JOIN sessions_bounced sb  ON slt.session_id = sb.session_id
      | GROUP BY date_format(slt.page_view_ts,'w')
      | ORDER BY start_date
      |
      |
      |""".stripMargin).show(100,false)



  println("Analyze conversion funnel for gsearch for lander-1 page (/products ,  /the-original-mr-fuzzy ,  /cart ,   /shipping , /billing ,  /thank-you-for-your-order ) using data between 2012-08-05 to 2012-09-05")

  spark.sql(
    """
      | SELECT
      | ws.website_session_id,
      | COUNT( CASE WHEN wp.pageview_url = '/products' THEN 1 ELSE NULL END ) as to_product,
      | COUNT( CASE WHEN wp.pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE NULL END ) as to_fuzzy,
      | COUNT( CASE WHEN wp.pageview_url = '/cart' THEN 1 ELSE NULL END ) as to_cart,
      | COUNT( CASE WHEN wp.pageview_url = '/shipping' THEN 1 ELSE NULL END ) as to_shipping,
      | COUNT( CASE WHEN wp.pageview_url = '/billing' THEN 1 ELSE NULL END ) as to_billing,
      | COUNT( CASE WHEN wp.pageview_url = '/thank-you-for-your-order' THEN 1 ELSE NULL END ) as to_thankyou
      |
      | FROM website_pageviews wp
      | JOIN website_sessions ws ON ws.website_session_id = wp.website_session_id
      | WHERE ws.created_at BETWEEN  '2012-08-05' AND '2012-09-05' AND ws.utm_source = 'gsearch' AND ws.utm_campaign = 'nonbrand'
      | GROUP BY ws.website_session_id
      | ORDER BY ws.website_session_id
      |
      |""".stripMargin).createOrReplaceTempView("sessions_to_pagevisits")


  spark.sql(
    """
      |
      | SELECT
      | COUNT(stp.website_session_id) as total_sessions,
      | SUM(stp.to_product) / COUNT(stp.website_session_id) as lander_clickthrough,
      | SUM(stp.to_fuzzy) / SUM(stp.to_product) as product_clickthrough,
      | SUM(stp.to_cart) / SUM(stp.to_fuzzy)  as fuzzy_clickthrough,
      | SUM(stp.to_shipping) / SUM(stp.to_cart)  as cart_clickthrough,
      | SUM(stp.to_billing)/SUM(stp.to_shipping) as shipping_clickthrough,
      | SUM(stp.to_thankyou)/SUM(stp.to_billing) as billing_clickthrough
      |
      | FROM sessions_to_pagevisits stp
      |
      |""".stripMargin).show(100,truncate = false)

/*  +--------------+-------------------+--------------------+------------------+------------------+---------------------+--------------------+
  |total_sessions|lander_clickthrough|product_clickthrough|fuzzy_clickthrough|cart_clickthrough |shipping_clickthrough|billing_clickthrough|
  +--------------+-------------------+--------------------+------------------+------------------+---------------------+--------------------+
  |4493          |0.4707322501669263 |0.7408983451536643  |0.4358647096362476|0.6661786237188873|0.7934065934065934   |0.4376731301939058  |
  +--------------+-------------------+--------------------+------------------+------------------+---------------------+--------------------+*/


  println("billing-2 vs billing page conversion for all traffic as of 2012-10-10")

  //Step 1- Finding when billing-2 was first visited.

  spark.sql(
    """
      |
      |SELECT MIN(DATE(created_at)) as first_launch_date
      |FROM website_pageviews
      |WHERE pageview_url = '/billing-2'
      |
      |""".stripMargin).show(10,false)


  spark.sql(
    """
      |
      | SELECT
      | wp.pageview_url,
      | COUNT(DISTINCT wp.website_session_id) as sessions,
      | COUNT(DISTINCT o.website_session_id) as orders,
      | COUNT(DISTINCT o.website_session_id) /  COUNT(DISTINCT wp.website_session_id) as ratio
      |
      |
      | FROM website_pageviews wp
      | LEFT JOIN orders o ON o.website_session_id = wp.website_session_id
      | WHERE wp.created_at BETWEEN '2012-09-10' AND '2012-11-10' AND wp.pageview_url IN ('/billing','/billing-2')
      | GROUP BY wp.pageview_url
      | ORDER BY wp.pageview_url
      |
      |
      |""".stripMargin).show(10,truncate = false)


//      +------------+--------+------+------------------+
//      |pageview_url|sessions|orders|ratio             |
//      +------------+--------+------+------------------+
//      |/billing    |657     |300   |0.45662100456621  |
//      |/billing-2  |654     |410   |0.6269113149847095|
//      +------------+--------+------+------------------+


  spark.close()

}
