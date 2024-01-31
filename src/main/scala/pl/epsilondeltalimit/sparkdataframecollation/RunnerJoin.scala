package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import pl.epsilondeltalimit.sparkdataframecollation.implicits._

object RunnerJoin {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder
      .master("local[2]")
      .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j2.properties")
      .getOrCreate()
    implicit val norm: Norm =
      Norm(normCase = Norm.Case.Upper, normTrim = Norm.Trim.Trim, normAccent = Norm.Accent.Strip)

    import spark.implicits._

    val df = Seq("a", "A").toDF()
    df.show()

    val right = Seq("a").toDF()

    println("=== JOIN 1 ===")
    df.join(right, df("value") === right("value"), "left").show()
    df.as("left").c.join(right.as("right"), col("left.value") === col("right.value"), "left").df.show()

    println("=== JOIN 2 ===")
    df.join(right, df("value") === right("value") && df("value") === "a", "left").show()
    df.as("left")
      .c
      .join(right.as("right"), col("left.value") === col("right.value") && col("left.value") === "a", "left")
      .df
      .show()

  }

}
