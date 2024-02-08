package pl.epsilondeltalimit.sparkdataframecollation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollationDataFrameDistinctSpec extends AnyFlatSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.implicits._
  import spark.implicits._

  implicit val norm: Norm = Norm(normCase = Norm.Case.Upper)

  behavior of "distinct"

  it should "fallback to dataframe distinct when called on a dataframe without normalized columns" in {
    val df = Seq(1, 1, 2).toDF()

    val r = df.c.distinct()

    r.schema should ===(df.schema)
    r.as[Int].collect().sorted should ===(Array(1, 2))
  }

  it should "deduplicate dataframe when called on a dataframe with normalized columns" in {
    val df = Seq("a", "A", "b").toDF()

    val r = df.c.distinct()

    r.schema should ===(df.schema)
    r.as[String].collect().sorted should ===(Array("A", "B"))
  }

  it should "deduplicate dataframe when called on a dataframe with normalized and non-normalized columns" in {
    val df = Seq(("a", 1), ("A", 1), ("b", 2)).toDF()

    val r = df.c.distinct()

    r.schema should ===(df.schema)
    r.as[(String, Int)].collect().sorted should ===(Array(("A", 1), ("B", 2)))
  }

}
