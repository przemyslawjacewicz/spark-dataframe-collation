package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import pl.epsilondeltalimit.sparkdataframecollation.collation.CollationDataFrame
import pl.epsilondeltalimit.sparkdataframecollation.collation.implicits._
import pl.epsilondeltalimit.sparkdataframecollation.commons.implicits._
import pl.epsilondeltalimit.sparkdataframecollation.normalization.Norm
import pl.epsilondeltalimit.sparkdataframecollation.normalization.implicits._

object Runner {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder
      .master("local[2]")
      .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j2.properties")
      .getOrCreate()
    implicit val norm: Norm = Norm(stringNorm = Norm.StringNorm(caseNorm = Norm.StringNorm.Case.Upper))

    import spark.implicits._

    val df = Seq("a", "A").toDF()
//    val df = new CollationDataFrame(Seq("a", "A").toDF())
    df.show()

    val right = Seq("a").toDF()
    right.show()

    df.join(right).show()

//    val raw = df
//      .toDF()
//      .as("left")
//      .join(right.toDF().as("right"), col("left.value") === col("right.value"), "left")
//    println(raw.getAlias)
//    raw.show()
//    raw.select("x").show()

//    val r = df
//      .as("left")
//      .join(right.as("right"), col("left.value") === col("right.value"), "left")
//    println(r.getAlias)
//    r.show()
//    r.select("x").show()
//      .where(col("left.value") === "a")

  }

}
