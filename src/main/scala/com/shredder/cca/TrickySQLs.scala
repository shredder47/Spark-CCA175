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

  /* ------------------------------------------------------------------------------------------------------------------*/

  /* Generate the following two result sets:

     Query an alphabetically ordered list of all names in OCCUPATIONS, immediately followed by the first letter of each profession as a parenthetical (i.e.: enclosed in parentheses). For example: AnActorName(A), ADoctorName(D), AProfessorName(P), and ASingerName(S).
     Query the number of ocurrences of each occupation in OCCUPATIONS. Sort the occurrences in ascending order, and output them in the following format:

     There are a total of [occupation_count] [occupation]s.
     where [occupation_count] is the number of occurrences of an occupation in OCCUPATIONS and [occupation] is the lowercase occupation name. If more than one Occupation has the same [occupation_count], they should be ordered alphabetically.

     The results of the second query are ascending ordered first by number of names corresponding to each profession , and then alphabetically by profession
  */

  Seq(
    ("Samantha","Doctor"),
    ("Julia","Actor"),
    ("Maria","Actor"),
    ("Meera","Singer"),
    ("Ashley","Professor"),
    ("Ketty","Professor"),
    ("Christeen","Professor"),
    ("Jane","Actor"),
    ("Jenny","Doctor"),
    ("Priya","Singer"),
  ).toDF("Name","Occupation").createOrReplaceTempView("occupations")

  /** NOTE : The use Of LIMIT is important here to retain the ordering after UNION Operation for Traditional SQL engines also for SPARK SQL */
  spark.sql(
    """
      | (
      |     SELECT CONCAT(name, '(', LEFT(Occupation, 1), ')') as row_one
      |     FROM (
      |              SELECT name, occupation
      |              FROM occupations
      |              ORDER BY name ASC
      |              LIMIT 1000
      |          ) as tmp
      | )
      | UNION ALL
      | (
      |     SELECT CONCAT('There are a total of ', cnt, ' ', prop, 's.') as row_one
      |     FROM (
      |              SELECT COUNT(Occupation) as cnt, LOWER(Occupation) as prop
      |              FROM occupations
      |              GROUP BY LOWER(Occupation)
      |              ORDER BY 1, 2 ASC
      |              LIMIT 1000
      |          ) as tmp
      | )
      |""".stripMargin).show(100,truncate = false)
}
