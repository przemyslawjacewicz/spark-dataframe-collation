package pl.epsilondeltalimit.sparkdataframecollation.collation

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.TableFor3
import org.scalatest.prop.Tables.Table
import org.scalatest.propspec.AnyPropSpec
import pl.epsilondeltalimit.sparkdataframecollation.SparkSessionProvider
import pl.epsilondeltalimit.sparkdataframecollation.normalization.Norm

class CollationDataset__Where__ConditionExpr__Spec extends AnyPropSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.collation.implicits._
  import spark.implicits._

  // todo: find a way to abstract over norm config - stub/mock ?
  val norm: Norm = Norm(stringNorm = Norm.StringNorm(caseNorm = Norm.StringNorm.Case.Upper))

  val df: Dataset[Row] = Seq((1, "a"), (2, "b")).toDF().as("df")

  val tests: TableFor3[String, Norm, Array[Row]] = Table(
    ("condition expr", "norm", "expected"),
    ("_1 = 1", Norm(), Array(Row(1, "a"))),
    ("df._1 = 1", Norm(), Array(Row(1, "a"))),
    ("_1 = 1", norm, Array(Row(1, "a"))),
    ("df._1 = 1", norm, Array(Row(1, "a"))),
    ("_2 = 'A'", Norm(), Array()),
    ("df._2 = 'A'", Norm(), Array()),
    ("_2 = 'A'", norm, Array(Row(1, "a"))),
    ("df._2 = 'A'", norm, Array(Row(1, "a")))
  )

  property("filter should filter with normalization") {
    forAll(tests) { case (conditionExpr, norm, expected) =>
      implicit val n: Norm = norm

      val r = df.c.filter(conditionExpr)

      r.schema should ===(df.schema)
      r.collect() should contain theSameElementsAs expected
    }
  }

}
