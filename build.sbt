ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "io.github.funrep"
// Versions come from git tags (sbt-dynver, via sbt-ci-release).
ThisBuild / homepage     := Some(url("https://github.com/funrep/strom"))
ThisBuild / scmInfo      := Some(ScmInfo(url("https://github.com/funrep/strom"), "scm:git@github.com:funrep/strom.git"))
ThisBuild / developers   := List(Developer("funrep", "Karl-Oskar Rikås", "2212965+funrep@users.noreply.github.com", url("https://github.com/funrep")))
ThisBuild / licenses     := List("MIT" -> url("https://opensource.org/licenses/MIT"))
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
// Scaladoc for the library modules: hide the macro internals, link to the sources on GitHub.
lazy val docSettings = Seq(
  Compile / doc / scalacOptions ++= Seq(
    "-skip-by-id:strom.internal",
    "-source-links:github://funrep/strom/main",
  ),
)

lazy val root = (project in file("."))
  .aggregate(core, fs2, zio, loom, bench)
  .settings(publish / skip := true)

// strom-core: zero dependencies beyond the Scala 3 standard library.
lazy val core = (project in file("core"))
  .settings(docSettings)
  .settings(
    name := "strom-core",
    libraryDependencies ++= Seq(
      "org.scalameta"  %% "munit"            % "1.3.6"             % Test,
      "org.scalacheck" %% "scalacheck"       % "1.20.0"            % Test,
      "org.scalameta"  %% "munit-scalacheck" % "1.3.1"             % Test,
      "org.scala-lang" %% "scala3-staging"   % scalaVersion.value  % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= Seq("-Xss8m"),
    Test / envVars ++= Seq("STROM_UPDATE_GOLDEN", "STROM_SHAPE_BATCHES").flatMap(k => sys.env.get(k).map(k -> _)).toMap,
  )

core / Test / scalacOptions ++= Seq("-Xcheck-macros")

lazy val testDeps = Seq(
  "org.scalameta"  %% "munit"            % "1.3.6" % Test,
  "org.scalameta"  %% "munit-scalacheck" % "1.3.1" % Test,
)

// strom-fs2: runs compiled segments inside fs2 streams (Pull/uncons, cede, Chunk.array).
lazy val fs2 = (project in file("fs2"))
  .settings(docSettings)
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name := "strom-fs2",
    libraryDependencies ++= Seq("co.fs2" %% "fs2-core" % "3.14.0") ++ testDeps,
    Test / fork := true,
    Test / sourceGenerators += Def.task(FuzzGen.fs2((Test / sourceManaged).value)).taskValue,
  )

// strom-zio: runs compiled segments as ZIO stream pipelines (ZChannel, yieldNow, Chunk.fromArray).
lazy val zio = (project in file("zio"))
  .settings(docSettings)
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name := "strom-zio",
    libraryDependencies ++= Seq("dev.zio" %% "zio-streams" % "2.1.26") ++ testDeps,
    Test / fork := true,
    Test / sourceGenerators += Def.task(FuzzGen.zio((Test / sourceManaged).value)).taskValue,
  )

// strom-loom: runs compiled segments on JDK virtual threads over blocking queues. No dependencies.
lazy val loom = (project in file("loom"))
  .settings(docSettings)
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name := "strom-loom",
    libraryDependencies ++= testDeps,
    Test / fork := true,
  )

// JMH benchmarks: fused Ström vs plain fs2 / ZStream (not published).
lazy val bench = (project in file("bench"))
  .dependsOn(core % "compile->compile;compile->test", fs2, zio, loom)
  .enablePlugins(JmhPlugin)
  .settings(
    name := "strom-bench",
    publish / skip := true,
    libraryDependencies += "dev.zio" %% "zio-streams" % "2.1.26",
  )

// Prototype, not part of Ström: fuse plain cats-effect IO blocks (proto/iofuse/README.md).
// Not aggregated into root and not published.
lazy val iofuse = (project in file("proto/iofuse"))
  .settings(
    name := "iofuse",
    publish / skip := true,
    libraryDependencies ++= Seq("org.typelevel" %% "cats-effect" % "3.7.1", "dev.zio" %% "zio" % "2.1.26") ++ testDeps,
    // Virtual time for the tests around sleep and timeouts.
    libraryDependencies ++= Seq("org.typelevel" %% "cats-effect-testkit" % "3.7.1" % Test, "dev.zio" %% "zio-test" % "2.1.26" % Test),
    Test / fork := true,
    Test / scalacOptions += "-Xcheck-macros",
    Test / sourceGenerators += Def.task(IOFuzzGen((Test / sourceManaged).value)).taskValue,
    // The generated fuzz programs call defs by value after effects on purpose: no notes for those.
    Test / compile := (Test / compile).dependsOn(Def.task { sys.props("iofuse.notes") = "off" }).value,
  )

lazy val iofuseBench = (project in file("proto/iofuse/bench"))
  .dependsOn(iofuse)
  .enablePlugins(JmhPlugin)
  .settings(name := "iofuse-bench", publish / skip := true)
