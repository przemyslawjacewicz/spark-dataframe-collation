package pl.epsilondeltalimit.sparkdataframecollation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollationDataFrameSpec extends AnyFlatSpec with SparkSessionProvider with NormProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.implicits._
  import spark.implicits._

  behavior of "distinct"

  it should "return the same dataframe when called on a dataframe without string columns" in {
    val df = Seq(1, 1, 2).toDF()

    df.c.distinct().schema should ===(df.schema)
    df.c.distinct().as[Int].collect().sorted should ===(Array(1, 2))
  }

  it should "return deduplicated dataframe when called on a dataframe with string columns" in {
    val df = Seq("a", "A", "b").toDF()

    df.c.distinct().schema should ===(df.schema)
    df.c.distinct().as[String].collect().sorted should ===(Array(norm"a", norm"b"))
  }

  it should "return deduplicated dataframe when called on a dataframe with string and non-string columns" in {
    val df = Seq(("a", 1), ("A", 1), ("b", 2)).toDF()

    df.c.distinct().schema should ===(df.schema)
    df.c.distinct().as[(String, Int)].collect().sorted should ===(Array((norm"a", 1), (norm"b", 2)))
  }


}
