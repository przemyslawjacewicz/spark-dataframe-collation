case class ProjectVersion(major: Int, minor: Int, snapshot: Boolean = false) {
  def fullVersion = s"$major.$minor${if (snapshot) "-SNAPSHOT" else ""}"
}