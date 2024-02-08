package pl.epsilondeltalimit.sparkdataframecollation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollationDataFrameWhereConditionSpec extends AnyFlatSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.implicits._
  import spark.implicits._

  implicit val norm: Norm = Norm(normCase = Norm.Case.Upper)

  behavior of "where"

  it should "fallback to dataframe filter when filtering using non-normalized columns" in {
    val df = Seq((1, "a"), (1, "A"), (2, "b")).toDF()

    val r = df.c.where(df("_1") === 1)

    r.schema should ===(df.schema)
    r.as[(Int, String)].collect().sorted should ===(Array((1, "A"), (1, "a")))
  }

  it should "filter dataframe with normalization when filtering using normalized columns" in {
    val df = Seq((1, "a"), (1, "A"), (2, "b")).toDF()

    val r = df.c.where(df("_2") === "a")

    r.schema should ===(df.schema)
    r.as[(Int, String)].collect().sorted should ===(Array((1, "A"), (1, "a")))
  }

}
