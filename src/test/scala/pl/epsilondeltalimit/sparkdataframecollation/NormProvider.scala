package pl.epsilondeltalimit.sparkdataframecollation

trait NormProvider {
  implicit lazy val norm: Norm =
    Norm(normCase = Norm.Case.Upper, normTrim = Norm.Trim.Trim, normAccent = Norm.Accent.Strip)
}
