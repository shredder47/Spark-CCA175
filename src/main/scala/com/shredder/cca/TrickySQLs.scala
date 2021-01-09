package com.shredder.cca

import org.apache.spark.sql.SparkSession

object TrickySQLs extends App {

  val spark = SparkSession
    .builder()
    .appName("CCA Practice")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  Seq(
    (1,"Henry"),
    (2,"Kevin"),
    (3,"Xander"),
    (4,"Ivan"),
    (5,"Troy")
  ).toDF("id","name")createOrReplaceTempView("tbl1")

  /* Implement LAG and LEAD without using it */
  spark.sql(
    """
      |
      | SELECT t0.Id,t0.name as current_name,t1.Name as prev_name, t2.Name as next_name
      | FROM tbl1 t0
      | LEFT JOIN tbl1 t1 ON t0.id = t1.Id + 1
      | LEFT JOIN tbl1 t2 ON t0.id +1 = t2.Id
      |
      |""".stripMargin).show(100)


  /* ------------------------------------------------------------------------------------------------------------------*/

  /* Delete Symmetrical Duplicates */

  Seq(
    (1,"Apple","Fruit",0.7f),
    (2,"Apple","Nut",0.2f),
    (3,"Fruit","Apple",0.7f),
    (4,"Car","Vehicle",0.9f),
    (5,"Vehicle","car",0.9f)
  ).toDF("id","word1","word2","match_pct").createOrReplaceTempView("synonyms")

  println("With Duplicates")
  spark.sql("SELECT * FROM synonyms").show(100)

  println("With out Duplicates")

  spark.sql(
    """
      | SELECT id,  word1,  word2,match_pct
      |
      | FROM
      | (
      |   SELECT
      |     *,
      |    ROW_NUMBER() OVER (PARTITION BY normalized_word,match_pct  ORDER BY normalized_word,match_pct) as rn
      |
      |   FROM
      |   (
      |     SELECT
      |     *,
      |     CASE
      |       WHEN LOWER(word1) > LOWER(word2)
      |             THEN CONCAT_WS(" ",LOWER(word1),LOWER(word2))
      |             ELSE CONCAT_WS(" ",LOWER(word2),LOWER(word1))
      |     END as normalized_word
      |     FROM synonyms
      |   ) as tmp1
      | )as tmp2
      | WHERE tmp2.rn = 1
      | ORDER BY tmp2.id
      |
      |""".stripMargin).show(100)

}
