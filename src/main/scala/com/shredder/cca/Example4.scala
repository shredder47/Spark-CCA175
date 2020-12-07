package com.shredder.cca

import org.apache.spark.sql.SparkSession

object Example4 extends App {

  val spark = SparkSession
    .builder()
    .appName("CCA Practice")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /* WINDOW FUNCTIONS EXAMPLES */


  spark
    .read
    .orc("data/retail_db/products_orc")
    .createOrReplaceTempView("products")

  spark
    .read
    .option("inferSchema", value = true)
    .option("header",value = true)
    .csv("data/retail_db/categories-header")
    .createOrReplaceTempView("product_categories")


  spark.sql(
    """
      |
      | SELECT
      |   p.product_id,
      |   pc.category_id,
      |   pc.category_name,
      |   p.product_name,
      |   p.product_price
      |
      | FROM products p
      | LEFT JOIN product_categories pc ON p.product_category_id = pc.category_id
      | ORDER BY p.product_id
      |
      |
      |""".stripMargin).createOrReplaceTempView("prods")

    spark.sql("SELECT * FROM prods").show(20,false)



  spark.stop()

}
