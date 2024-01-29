package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Runner {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[2]")
      .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j2.properties")
      .getOrCreate()

    import spark.implicits._

    val df = Seq("a", "A").toDF()
    df.show()

    println("=== DISTINCT ===")
    df.c.distinct().df.show()

    println("=== JOIN ===")
    val right = Seq("a").toDF()
    df.join(right, df("value") === right("value"), "left").show()
    df.as("left").c.join(right.as("right"), col("left.value") === col("right.value"), "left").df.show()

    println("=== WHERE ===")
    df.where(col("value") === "a").show()
    df.c.where(col("value") === "a").df.show()

    println("=== pipeline ===")
    df.as("left")
      .c
      .join(right.as("right"), col("left.value") === col("right.value"), "left")
      .where(col("left.value") === "a")
      .df
      .show()

  }

}
