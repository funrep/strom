// A standalone project consuming the locally published Ström artifacts (`sbt publishLocal` in
// the parent build first), the way an application would.
scalaVersion := "3.9.0"
libraryDependencies ++= Seq(
  "dev.strom" %% "strom-fs2" % "0.1.0-SNAPSHOT",
  "dev.strom" %% "strom-zio" % "0.1.0-SNAPSHOT",
)
