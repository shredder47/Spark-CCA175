package com.shredder.cca.util

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

class Connection(sparkSession: SparkSession, userName: String, password: String, dbName: String, url: String) {

  def getTableAsDataframe(tableName: String): DataFrame = {

    val properties = new Properties()
    properties.put("user", userName)
    properties.put("password", password)

    val fullUrl = url + dbName
    val df = sparkSession.read.jdbc(fullUrl, tableName, properties = properties)
    df
  }

}
