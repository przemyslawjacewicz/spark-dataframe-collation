package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollationDataFrameSpec extends AnyFlatSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.implicits._
  import spark.implicits._

  implicit val norm: Norm =
    Norm(normCase = Norm.Case.Upper, normTrim = Norm.Trim.Trim, normAccent = Norm.Accent.Strip)

  behavior of "distinct"

  ignore should "fallback to dataframe distinct when called on a dataframe without string columns" in {
    val df = Seq(1, 1, 2).toDF()

    val r = df.c.distinct()

    r.schema should ===(df.schema)
    r.as[Int].collect().sorted should ===(Array(1, 2))
  }

  ignore should "deduplicate dataframe when called on a dataframe with string columns" in {
    val df = Seq("a", "A", "b").toDF()

    val r = df.c.distinct()

    r.schema should ===(df.schema)
    r.as[String].collect().sorted should ===(Array(norm"a", norm"b"))
  }

  ignore should "deduplicate dataframe when called on a dataframe with string and non-string columns" in {
    val df = Seq(("a", 1), ("A", 1), ("b", 2)).toDF()

    val r = df.c.distinct()

    r.schema should ===(df.schema)
    r.as[(String, Int)].collect().sorted should ===(Array((norm"a", 1), (norm"b", 2)))
  }

  // note: testing only full outer join
  behavior of "join"

  ignore should "fallback to dataframe join when called on dataframes without string columns" in {
    val df    = Seq(1, 2).toDF()
    val right = Seq(1).toDF()

    val r = df.as("df").c.join(right.as("right"), col("df.value") === col("right.value"), "left")

    r.schema should ===(StructType.fromDDL("value INT, value INT"))
    r.as[(Int, Int)].collect().sorted should ===(Array((1, 1), (1, null)))
  }

  it should "join dataframes when called on dataframes with string columns" in {
    val df    = Seq("a", "B").toDF()
    val right = Seq("A").toDF()

    val r = df
      .as("df")
      .c
      .join(right.as("right"), col("df.value") === col("right.value"), "full_outer")
    r.show()

    r.as[(Option[String], Option[String])]
      .collect()
      .sorted should ===(Array((Some("B"), None), (Some("a"), Some("A"))))
  }
}
