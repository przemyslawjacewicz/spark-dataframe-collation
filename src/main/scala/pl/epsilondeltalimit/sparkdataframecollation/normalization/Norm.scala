package pl.epsilondeltalimit.sparkdataframecollation.normalization

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import pl.epsilondeltalimit.sparkdataframecollation.commons.implicits.StringWithStripAccents

case class Norm(stringNorm: Norm.StringNorm = Norm.StringNorm()) {

  //todo: fix dataframe/dataset param
  def apply(c: Column, df: Dataset[_]): Column = {
    // note: applicable for leaf expression (no children)
    def applyForLeaf(expr: Expression): Expression = {
//      println(s"apply: expr=$expr")
      val r = df.selectExpr(expr.sql).schema.head.dataType match {
        case StringType =>
          df.queryExecution.analyzed.output.find(_.sql.endsWith(expr.sql)) match {
            case Some(a) => stringNorm(col(a.sql)).expr
            case None    => stringNorm(lit(expr.sql)).expr
          }
        case _ => expr
      }

//      println(s"apply: r=$r")
      r
    }

    def go(expr: Expression): Expression =
      if (expr.children.isEmpty) applyForLeaf(expr)
      else expr.mapChildren(e => if (e.children.isEmpty) applyForLeaf(e) else go(e))

    new Column(go(c.expr))
  }

}

object Norm {

  trait Apply[A] {
    def apply(a: A): A
    def apply(c: Column): Column
  }

  // todo: either use case + trim + accent, or custom
  case class StringNorm(caseNorm: Norm.StringNorm.Case = Norm.StringNorm.Case.None,
                        trimNorm: Norm.StringNorm.Trim = Norm.StringNorm.Trim.None,
                        accentNorm: Norm.StringNorm.Accent = Norm.StringNorm.Accent.None,
                        customNorm: Norm.StringNorm.Custom = Norm.StringNorm.Custom(identity))
      extends Apply[String] {

    override def apply(a: String): String =
      accentNorm(trimNorm(caseNorm(a)))

    override def apply(c: Column): Column =
      accentNorm(trimNorm(caseNorm(c)))
  }

  object StringNorm {
    sealed trait Case extends Apply[String] with Product with Serializable {
      override def apply(s: String): String =
        this match {
          case Case.Upper => Option(s).map(_.toUpperCase).orNull
          case Case.Lower => Option(s).map(_.toLowerCase).orNull
          case Case.None  => s
        }

      override def apply(c: Column): Column =
        this match {
          case Case.Upper => upper(c)
          case Case.Lower => lower(c)
          case Case.None  => c
        }
    }

    object Case {
      case object Upper extends Case

      case object Lower extends Case

      case object None extends Case
    }

    sealed trait Trim extends Apply[String] with Product with Serializable {
      override def apply(s: String): String =
        this match {
          case Trim.LTrim => Option(s).map(_.replaceAll("^\\s", "")).orNull
          case Trim.Trim  => Option(s).map(_.replaceAll("^\\s+|\\s+$", "")).orNull
          case Trim.RTrim => Option(s).map(_.replaceAll("\\s+$", "")).orNull
          case Trim.None  => s
        }

      override def apply(c: Column): Column =
        this match {
          case Trim.LTrim => ltrim(c)
          case Trim.Trim  => trim(c)
          case Trim.RTrim => rtrim(c)
          case Trim.None  => c
        }
    }

    object Trim {
      case object LTrim extends Trim

      case object Trim extends Trim

      case object RTrim extends Trim

      case object None extends Trim
    }

    sealed trait Accent extends Apply[String] with Product with Serializable {
      override def apply(s: String): String =
        this match {
          case Accent.Strip => Option(s).map(_.stripAccents).orNull
          case Accent.None  => s
        }

      override def apply(c: Column): Column =
        this match {
          case Accent.Strip => udf((colValue: String) => Option(colValue).map(_.stripAccents)).apply(c)
          case Accent.None  => c
        }
    }

    object Accent {
      case object Strip extends Accent

      case object None extends Accent
    }

    case class Custom(f: String => String) extends Apply[String] {
      override def apply(a: String): String =
        f(a)

      override def apply(c: Column): Column =
        udf((colValue: String) => Option(colValue).map(f)).apply(c)
    }

  }

}
