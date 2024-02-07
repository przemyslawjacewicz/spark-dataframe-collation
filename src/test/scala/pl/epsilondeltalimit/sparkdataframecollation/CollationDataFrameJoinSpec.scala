package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollationDataFrameJoinSpec extends AnyFlatSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.implicits._
  import spark.implicits._

  // todo: find a way to abstract over norm config
  // stub/mock ?
  implicit val norm: Norm =
    Norm(normCase = Norm.Case.Upper, normTrim = Norm.Trim.Trim, normAccent = Norm.Accent.Strip)

  // note: testing only full outer join
  behavior of "join using usingColumns"

  behavior of "join using usingColumn"

  behavior of "join using usingColumn"

  it should "fallback to dataframe join when called on dataframes without string columns" in {
    val df    = Seq(1, 2).toDF()
    val right = Seq(1).toDF()

    val r = df.c.join(right, "value")

    r.schema should ===(StructType.fromDDL("value INT NOT NULL, value INT NOT NULL"))
    r.as[(Int, Int)].collect().sorted should ===(Array((1, 1)))
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

  behavior of "join using joinExprs"

  it should "fallback to dataframe join when called on dataframes without string columns" in {
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

  // todo: add test for string and non-string columns
}
