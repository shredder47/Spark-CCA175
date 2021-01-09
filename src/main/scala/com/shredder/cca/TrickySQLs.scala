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

  val df = Seq(
    (1,"Henry"),
    (2,"Kevin"),
    (3,"Xander"),
    (4,"Ivan"),
    (5,"Troy")
  ).toDF("id","name")

  df.createOrReplaceTempView("tbl1")

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

}
