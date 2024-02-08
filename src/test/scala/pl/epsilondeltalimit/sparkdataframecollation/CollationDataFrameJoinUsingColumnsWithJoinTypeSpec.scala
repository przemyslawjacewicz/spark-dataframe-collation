package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollationDataFrameJoinUsingColumnsWithJoinTypeSpec extends AnyFlatSpec with SparkSessionProvider with Matchers {

  import pl.epsilondeltalimit.sparkdataframecollation.implicits._
  import spark.implicits._

  // todo: find a way to abstract over norm config - stub/mock ?
  implicit val norm: Norm = Norm(normCase = Norm.Case.Upper)

  behavior of "join"

  it should "fallback to dataframe join when joined using non-normalized columns" in {
    val df    = Seq((1, "a"), (2, "b")).toDF()
    val right = Seq((1, "A"), (-2, "-B")).toDF()

    val r = df.c.join(right, Seq("_1"), "full_outer")

    r.schema should ===(StructType.fromDDL("_1 INT, _2 STRING, _2 STRING"))
    r.as[(Int, String, String)].collect().sorted should ===(Array((-2, null, "-B"), (1, "a", "A"), (2, "b", null)))
  }

  it should "join dataframes with normalization when joined using normalized columns" in {
    val df    = Seq((1, "a"), (2, "b")).toDF()
    val right = Seq((1, "A"), (-2, "-B")).toDF()

    val r = df.c.join(right, Seq("_2"), "full_outer")

    r.schema should ===(StructType.fromDDL("_2 STRING, _1 INT, _1 INT"))
    r.as[(String, Option[Int], Option[Int])].collect().sorted should ===(
      Array(("-B", None, Some(-2)), ("A", Some(1), Some(1)), ("B", Some(2), None))
    )
  }

}
