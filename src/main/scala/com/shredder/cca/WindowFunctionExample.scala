package com.shredder.cca

import org.apache.spark.sql.SparkSession

object WindowFunctionExample extends App {

  val spark = SparkSession
    .builder()
    .appName("CCA Practice")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


  /* WINDOW FUNCTIONS EXAMPLES */

  Seq(
    (10,"2012-01-15",1,1),
    (11,"2012-01-16",1,1),
    (12,"2012-01-17",1,0),
    (13,"2012-01-18",1,1),
    (14,"2012-01-19",1,1),
    (15,"2012-01-20",1,0),
    (16,"2012-01-21",1,0),
    (17,"2012-01-22",1,0),
    (18,"2012-01-23",1,0),
    (19,"2012-01-24",1,0),
    (20,"2012-01-25",1,0),
    (21,"2012-01-26",1,1),
    (22,"2012-01-27",1,0),
    (23,"2012-01-28",1,0),
    (24,"2012-01-29",1,1),
    (25,"2012-01-30",1,1),
    (26,"2012-01-15",2,1),
    (27,"2012-01-16",2,0),
    (28,"2012-01-17",2,0),
    (29,"2012-01-19",2,0),
    (30,"2012-01-18",2,1),
  ).toDF("id","date","emp_id","is_absent").createOrReplaceTempView("attendance")


  spark.sql(
    """
      |SELECT * FROM attendance
      |""".stripMargin).show(100,truncate = false)

  spark.sql(
    """
      |
      |SELECT
      | emp_id,
      | date,
      | is_absent,
      | CASE WHEN is_absent = 1 THEN true ELSE false END as t_0,
      | CASE WHEN LEAD(is_absent,1) OVER (PARTITION BY emp_id ORDER BY date ) = 1 THEN true ELSE false END as t_1,
      | CASE WHEN LEAD(is_absent,2) OVER (PARTITION BY emp_id ORDER BY date ) = 1 THEN true ELSE false END as t_2,
      | CASE WHEN LEAD(is_absent,3) OVER (PARTITION BY emp_id ORDER BY date ) = 1 THEN true ELSE false END as t_3
      |FROM attendance
      |ORDER BY emp_id, date
      |
      |
      |""".stripMargin).show(100,truncate = false)





  spark.stop()


}
