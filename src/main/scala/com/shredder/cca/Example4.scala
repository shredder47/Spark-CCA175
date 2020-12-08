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
    .option("inferSchema", value = true)
    .option("header",value = true)
    .csv("data/retail_db/sakila_payments.csv")
    .createOrReplaceTempView("sakila_payments")

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
      | INNER JOIN product_categories pc ON p.product_category_id = pc.category_id
      | ORDER BY p.product_id
      |
      |
      |""".stripMargin).createOrReplaceTempView("prods")

  /*+----------+-----------+-------------+---------------------------------------------+-------------+
  |product_id|category_id|category_name|product_name                                 |product_price|
  +----------+-----------+-------------+---------------------------------------------+-------------+
  |1         |2          |Soccer       |Quest Q64 10 FT. x 10 FT. Slant Leg Instant U|59.98        |
  |2         |2          |Soccer       |Under Armour Men's Highlight MC Football Clea|129.99       |
  |3         |2          |Soccer       |Under Armour Men's Renegade D Mid Football Cl|89.99        |
  |4         |2          |Soccer       |Under Armour Men's Renegade D Mid Football Cl|89.99        |
  |5         |2          |Soccer       |Riddell Youth Revolution Speed Custom Footbal|199.99       |
  +----------+-----------+-------------+---------------------------------------------+-------------+*/

  /*********
   * RANKS *
   *********/
  println("Rank Item in each category By its price")

  spark.sql(
    """
      |
      | SELECT
      |  *,
      |  ROW_NUMBER() OVER ( PARTITION BY category_id ORDER BY product_price DESC ) AS row_number,
      |  RANK() OVER ( PARTITION BY category_id ORDER BY product_price DESC ) AS rnk,
      |  DENSE_RANK() OVER ( PARTITION BY category_id ORDER BY product_price DESC ) AS dense_rnk
      |
      | FROM prods
      |
      | ORDER BY category_id, rnk
      |

      |""".stripMargin).createOrReplaceTempView("prods_w_ranks")

  println("prods_w_ranks")

  spark.sql(
    """
      | SELECT
      |   *
      | FROM prods_w_ranks
      |
      |""".stripMargin).show(10,truncate = false)

  println("Most Costly Items in Each Category")

  spark.sql(
    """
      | SELECT
      |   *
      | FROM prods_w_ranks
      |
      | WHERE dense_rnk = 1
      | ORDER BY product_id
      |
      |""".stripMargin).show(20,truncate = false)

  /*****************
   * MIN, MAX, AVG *
   *****************/

  println("Average, Max, Min Price for each Category")

  spark.sql(
    """
      |SELECT
      |  *,
      | MAX(product_price) OVER ( PARTITION BY category_id ) AS max_price_of_cat,
      | MIN(product_price) OVER ( PARTITION BY category_id ) AS min_price_of_cat,
      | AVG(product_price) OVER ( PARTITION BY category_id ) AS avg_price_of_cat
      |
      | FROM prods
      |
      | ORDER BY category_id, product_price
      |
      |""".stripMargin).show(20,truncate = false)

  println("*"*100)
  println("Sakila_Payment Table")
  spark.sql(
    """
      |SELECT
      |  *
      |FROM sakila_payments
      |
      |
      |""".stripMargin).show(15,truncate = false)


  /**********************
   * MIN, MAX, AVG ,SUM *
   **********************/

  println("Average, Max, Min payment of each customer!")

  spark.sql(
    """
      | SELECT
      |   payment_date,
      |   customer_id,
      |   amount,
      |   MIN(amount) OVER ( PARTITION BY customer_id ) as min_payment,
      |   MAX(amount) OVER ( PARTITION BY customer_id ) as max_payment,
      |   SUM(amount) OVER ( PARTITION BY customer_id ) as total_payment,
      |   AVG(amount) OVER ( PARTITION BY customer_id ) as avg_payment
      |
      | FROM sakila_payments
      | ORDER BY customer_id,payment_date
      |
      |""".stripMargin).show(50,truncate = false)


  /**********************
   * Running Total      *
   * Running Avg        *
   * NTiles             *
   **********************/

  println("Payment's Running Total,AVG for each customer!")

  spark.sql(
    """
      | SELECT
      |   payment_date,
      |   customer_id,
      |   amount,
      |   SUM(amount) OVER ( PARTITION BY customer_id ORDER BY payment_date ) as running_total,
      |   AVG(amount) OVER ( PARTITION BY customer_id ORDER BY payment_date ) as running_avg,
      |   NTILE(3) OVER ( PARTITION BY customer_id ORDER BY payment_date ) as tile
      |
      | FROM sakila_payments
      | ORDER BY customer_id,payment_date
      |
      |""".stripMargin).show(50,truncate = false)


  /**************************************************************
   * Running Total Using preceding and current Row              *
   * Running Avg  Using preceding and current Row             *
   **************************************************************/

  println("Payment's Running Total,AVG  Using  current Row and N Following for each customer !")

  spark.sql(
    """
      | SELECT
      |   payment_date,
      |   customer_id,
      |   amount,
      |   SUM(amount) OVER ( PARTITION BY customer_id ORDER BY payment_date ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING  ) as running_total1_f,
      |   SUM(amount) OVER ( PARTITION BY customer_id ORDER BY payment_date ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING  ) as running_total2_f,
      |   SUM(amount) OVER ( PARTITION BY customer_id ORDER BY payment_date ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING  ) as running_total3_f

      | FROM sakila_payments
      | ORDER BY customer_id,payment_date
      |
      |""".stripMargin).show(50,truncate = false)


  println("Payment's Running Total,AVG  Using N Preceding and  current Row for each customer !")

  spark.sql(
    """
      | SELECT
      |   payment_date,
      |   customer_id,
      |   amount,
      |   SUM(amount) OVER ( PARTITION BY customer_id ORDER BY payment_date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW   ) as running_total1_p,
      |   SUM(amount) OVER ( PARTITION BY customer_id ORDER BY payment_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) as running_total2_p,
      |   SUM(amount) OVER ( PARTITION BY customer_id ORDER BY payment_date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW ) as running_total3_p

      | FROM sakila_payments
      | ORDER BY customer_id,payment_date
      |
      |""".stripMargin).show(50,truncate = false)



  /*********
   * LAG   *
   * LEAD  *
   *********/

  println("LAG and Lead of payment amount for each customer")

  spark.sql(
    """
      | SELECT
      |   payment_date,
      |   customer_id,
      |   amount,
      |   LAG(amount) OVER(PARTITION BY customer_id ORDER BY payment_date) AS prev_amount,
      |   LEAD(amount) OVER(PARTITION BY customer_id ORDER BY payment_date ) AS upcoming_amount
      | FROM sakila_payments
      | ORDER BY customer_id,payment_date
      |
      |""".stripMargin).show(30,false)

  println("2 LAG and 2Lead and NULL replaced with 0.00 of payment amount for each customer")

  spark.sql(
    """
      | SELECT
      |   payment_date,
      |   customer_id,
      |   amount,
      |   LAG(amount,2,0) OVER(PARTITION BY customer_id ORDER BY payment_date) AS prev_amount,
      |   LEAD(amount,2,0) OVER(PARTITION BY customer_id ORDER BY payment_date ) AS upcoming_amount
      | FROM sakila_payments
      | ORDER BY customer_id,payment_date
      |
      |""".stripMargin).show(30,false)



  spark.stop()

}
