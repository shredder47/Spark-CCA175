package com.shredder.cca

import com.shredder.cca.util.Connection
import org.apache.spark.sql.{DataFrame, SparkSession}

object Example1 extends App {

  val spark = SparkSession
    .builder()
    .appName("CCA Practice")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val connection = new Connection(
    spark,
    "root",
    "123456",
    "sakila",
    "jdbc:mysql://localhost:3306/")

  connection.getTableAsDataframe("actor").createOrReplaceTempView("actor")
  connection.getTableAsDataframe("address").createOrReplaceTempView("address")
  connection.getTableAsDataframe("category").createOrReplaceTempView("category")
  connection.getTableAsDataframe("city").createOrReplaceTempView("city")
  connection.getTableAsDataframe("country").createOrReplaceTempView("country")
  connection.getTableAsDataframe("customer").createOrReplaceTempView("customer")
  connection.getTableAsDataframe("film").createOrReplaceTempView("film")
  connection.getTableAsDataframe("film_actor").createOrReplaceTempView("film_actor")
  connection.getTableAsDataframe("film_category").createOrReplaceTempView("film_category")
  connection.getTableAsDataframe("film_text").createOrReplaceTempView("film_text")
  connection.getTableAsDataframe("inventory").createOrReplaceTempView("inventory")
  connection.getTableAsDataframe("language").createOrReplaceTempView("language")
  connection.getTableAsDataframe("payment").createOrReplaceTempView("payment")
  connection.getTableAsDataframe("rental").createOrReplaceTempView("rental")
  connection.getTableAsDataframe("staff").createOrReplaceTempView("staff")
  connection.getTableAsDataframe("store").createOrReplaceTempView("store")


  //Films by Rating
  spark.sql("SELECT rating ,count(film_id) AS total_numbers FROM film GROUP BY rating ORDER BY 2 DESC").show()

  //Films by Rating and Rental Rate
  spark.sql("SELECT rating ,rental_rate ,count(film_id) AS total_numbers FROM film GROUP BY rating,rental_rate ORDER BY rating").show()

  //Customer Details with Address
  spark.sql(
    """
      | SELECT customer.customer_id, customer.first_name, customer.last_name,customer.email , address.address FROM
      | customer
      | INNER JOIN
      | address ON customer.address_id = address.address_id ORDER BY customer.customer_id
      |
      |""".stripMargin).show(10, truncate = false)


  // List by Film Name, language and category name
  spark.sql(
    """
      |SELECT film.film_id, film.title, category.name AS film_category , language.name AS language
      |
      |FROM film
      |INNER JOIN language ON film.language_id = language.language_id
      |INNER JOIN film_category ON film.film_id = film_category.film_id
      |INNER JOIN category ON category.category_id = film_category.category_id
      |ORDER BY film.film_id
      |
      |""".stripMargin).show(10, truncate = false)


  //How many times each movies has been rented out

  spark.sql(
    """
      |
      | SELECT film.title,count(rental.rental_id) as total_rentals
      | FROM rental
      | INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
      | INNER JOIN film ON inventory.film_id = film.film_id
      | GROUP BY film.title
      | ORDER BY 2 DESC
      |
      |""".stripMargin).show(10, truncate = false)

  //Revenue per Video title
  spark.sql(
    """
      |
      | SELECT  film.title,count(rental.rental_id) as total_rentals, film.rental_rate, sum(film.rental_rate) as revenue
      | FROM rental
      | INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
      | INNER JOIN film ON inventory.film_id = film.film_id
      | GROUP BY film.title , film.rental_rate
      | ORDER BY 1
      |
      |""".stripMargin).show(10, truncate = false)

  //What customer made us the most money
  spark.sql(
    """
      |SELECT customer.customer_id,customer.first_name,customer.last_name, SUM(payment.amount)
      |FROM customer
      |INNER JOIN payment on customer.customer_id = payment.customer_id
      |GROUP BY customer.customer_id,customer.first_name,customer.last_name
      |ORDER BY 4  DESC
      |
      |""".stripMargin).show(10, truncate = false)

  //Revenue By Store!

  spark.sql(
    """
      | SELECT i.store_id, SUM(p.amount) as total_revenue
      | FROM rental r
      | INNER JOIN payment p ON r.rental_id = p.rental_id
      | INNER JOIN inventory i ON i.inventory_id = r.inventory_id
      |
      | GROUP BY i.store_id
      | ORDER BY 2
      |
      |""".stripMargin).show(10, truncate = false)


  //Rental Numbers Each Month
  spark.sql(
    """
      |
      | SELECT YEAR(r.rental_date), MONTH(r.rental_date),COUNT(r.rental_id) as total_rentals
      |
      | FROM rental r
      | GROUP BY 1,2
      | ORDER BY 3 DESC
      |""".stripMargin).show(10, truncate = false)

  // Last Rental Date By Each customers
  spark.sql(
    """
      |
      | SELECT r.customer_id, concat_ws(" ", c.first_name,c.last_name) as customer_Name , c.email , MAX(r.rental_date) as last_rental_date
      |
      | FROM rental r
      | INNER JOIN customer c ON c.customer_id = r.customer_id
      | GROUP BY r.customer_id,c.first_name,c.last_name,c.email
      | ORDER BY 1
      |""".stripMargin).show(10, truncate = false)

  //Revenue by Each Month
  spark.sql(
    """
      |
      | SELECT YEAR(r.rental_date), MONTH(r.rental_date),COUNT(r.rental_id) as total_rentals, SUM(p.amount) as total_revenue
      |
      | FROM rental r
      | INNER JOIN payment p on p.rental_id = r.rental_id
      | GROUP BY 1,2
      | ORDER BY 4 DESC
      |""".stripMargin).show(10, truncate = false)


  //Average Rentals per Month
  spark.sql(
    """
      |
      | SELECT YEAR(r.rental_date),
      |        MONTH(r.rental_date),
      |        COUNT(r.rental_id) as total_rentals,
      |        COUNT(r.customer_id) as total_unique_rentals ,
      |        COUNT(r.rental_id) /COUNT(distinct r.customer_id) as avg_renters
      |
      |
      | FROM rental r
      | INNER JOIN payment p on p.rental_id = r.rental_id
      | GROUP BY 1,2
      | ORDER BY 4 DESC
      |""".stripMargin).show(10, truncate = false)

  //Number of distinct movies rented per Month(Getting unique items is imp info, as it determines if revenue generators are evenly distributed)
  spark.sql(
    """
      |
      | SELECT YEAR(r.rental_date),
      |        MONTH(r.rental_date),
      |        COUNT(r.rental_id) as total_rentals,
      |        COUNT(distinct i.film_id) as unique_movie_renters,
      |        COUNT(r.rental_id)/COUNT(distinct i.film_id) as rental_per_film
      |
      |
      | FROM rental r
      | INNER JOIN inventory i on i.inventory_id = r.inventory_id
      |
      | GROUP BY 1,2
      | ORDER BY 4 DESC
      |""".stripMargin).show(10, truncate = false)

  //Number of rentals per category
  spark.sql(
    """
      |
      | SELECT cat.name as film_category , COUNT(r.rental_id) as total_rental , COUNT(r.rental_id) / COUNT(distinct r.customer_id) AS rentals_per_renter
      |
      | FROM rental r
      | INNER JOIN inventory i ON i.inventory_id = r.inventory_id
      | INNER JOIN film f ON f.film_id = i.film_id
      | INNER JOIN film_category fc ON fc.film_id = f.film_id
      | INNER JOIN category cat ON fc.category_id = cat.category_id
      |
      | GROUP BY cat.category_id,cat.name
      | ORDER BY 2 DESC
      |""".stripMargin).show(100, truncate = false)


  // Who rented at least 3 times
  spark.sql(
    """
      |
      | SELECT  r.customer_id as customer_id , COUNT(r.rental_id) as rented
      |
      | FROM rental r

      | GROUP BY r.customer_id
      |
      |""".stripMargin).show(100, truncate = false)

  // Revenue for Store 1 where film is rated pg-13 and R
  spark.sql(
    """
      |
      | SELECT  i.store_id,f.rating  ,SUM(p.amount) as revenue
      |
      | FROM rental r
      | INNER JOIN payment p ON r.rental_id = p.rental_id
      | INNER JOIN inventory i ON  r.inventory_id = i.inventory_id
      | INNER JOIN film f ON f.film_id = i.film_id
      |
      | GROUP BY i.store_id , f.rating
      | HAVING i.store_id = 1 AND f.rating IN ('PG-13','R')
      |
      |""".stripMargin).show(100, truncate = false)


  // Revenue by top renters
  spark.sql(
    """
      | SELECT r.customer_id, COUNT(r.rental_id) as total_rental, SUM(p.amount) AS revenue
      | FROM rental r
      | INNER JOIN payment p ON p.rental_id = r.rental_id
      | GROUP BY r.customer_id
      | HAVING r.customer_id IN
      |(
      | SELECT r.customer_id
      | FROM rental r
      | GROUP BY r.customer_id
      | HAVING COUNT(r.rental_id) > 20
      |)
      |ORDER BY 2 DESC
      |

      |""".stripMargin).show(2000, truncate = false)


  spark.close()

}
