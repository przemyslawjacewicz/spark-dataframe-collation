package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import pl.epsilondeltalimit.sparkdataframecollation.implicits._

object RunnerWhere {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder
      .master("local[2]")
      .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j2.properties")
      .getOrCreate()
    implicit val norm: Norm =
      Norm(normCase = Norm.Case.Upper, normTrim = Norm.Trim.Trim, normAccent = Norm.Accent.Strip)

    import spark.implicits._

    val df = Seq(("a", 1), ("A", 11)).toDF()
    df.show()

    println("=== WHERE 1 ===")
    df.where(col("_1") === "a").show()
    df.c.where(col("_1") === "a").df.show()

    println("=== WHERE 2 ===")
    df.where(col("_1") === "a" && col("_1") === "A").show()
    df.c.where(col("_1") === "a" && col("_1") === "A").df.show()

    println("=== WHERE 3 ===")
    df.where(col("_2") === 1).printSchema()
    df.where(col("_2") === 1).show()
    df.c.where(col("_2") === 1).df.printSchema()
    df.c.where(col("_2") === 1).df.show()

  }

}
