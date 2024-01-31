package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import pl.epsilondeltalimit.sparkdataframecollation.implicits._

object RunnerPipeline {

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

    println("=== pipeline ===")
    val right = Seq("a").toDF()
    df.as("left")
      .c
      .join(right.as("right"), col("left.value") === col("right.value"), "left")
      .where(col("left.value") === "a")
      .df
      .show()

  }

}
