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

  spark.sql(
    """
      |SELECT * FROM website_sessions
      |""".stripMargin).show()

































  spark.close()

}
