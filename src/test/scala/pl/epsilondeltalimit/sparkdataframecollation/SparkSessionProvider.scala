package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  implicit lazy val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
    SparkSession.builder
      .appName("TestSparkSession")
      .master("local[*]")
      .getOrCreate()
  )
}
