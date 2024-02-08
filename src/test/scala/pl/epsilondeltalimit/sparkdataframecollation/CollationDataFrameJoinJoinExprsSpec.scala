package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollationDataFrameJoinJoinExprsSpec extends AnyFlatSpec with SparkSessionProvider with Matchers {

  import pl.epsilondeltalimit.sparkdataframecollation.implicits._
  import spark.implicits._

  // todo: find a way to abstract over norm config - stub/mock ?
  implicit val norm: Norm = Norm(normCase = Norm.Case.Upper)

  behavior of "join"

  it should "fallback to dataframe join when joined using non-normalized columns" in {
    val df    = Seq((1, "a"), (2, "b")).toDF()
    val right = Seq((1, "A")).toDF()

    val r = df.as("df").c.join(right.as("right"), col("df._1") === col("right._1"))

    r.schema should ===(StructType.fromDDL("_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING"))
    r.as[(Int, String, Int, String)].collect().sorted should ===(Array((1, "a", 1, "A")))
  }

  it should "join dataframes with normalization when joined using normalized columns" in {
    val df    = Seq((1, "a"), (2, "b")).toDF()
    val right = Seq((1, "A")).toDF()

    val r = df.as("df").c.join(right.as("right"), col("df._2") === col("right._2"))

    r.schema should ===(StructType.fromDDL("_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING"))
    r.as[(Int, String, Int, String)].collect().sorted should ===(Array((1, "a", 1, "A")))
  }
}
