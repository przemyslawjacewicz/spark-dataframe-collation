package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.SparkSession
import pl.epsilondeltalimit.sparkdataframecollation.implicits._

object RunnerDistinct {

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

    println("=== DISTINCT ===")
    df.c.distinct().show()

  }

}
