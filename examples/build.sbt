// A standalone project consuming the locally published Ström artifacts (`sbt publishLocal` in
// the parent build first), the way an application would.
scalaVersion := "3.9.0"
// The newest locally published version (`sbt publishLocal` in the parent build), or -Dstrom.version=…
val stromVersion = sys.props.getOrElse("strom.version", "latest.integration")
libraryDependencies ++= Seq(
  "io.github.funrep" %% "strom-fs2" % stromVersion,
  "io.github.funrep" %% "strom-zio" % stromVersion,
)
