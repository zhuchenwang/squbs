import de.johoop.findbugs4sbt.FindBugs._

name := "squbs-zkcluster"

val akkaV = "2.3.10"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-remote" % akkaV,
  "com.typesafe.akka" %% "akka-slf4j" % akkaV,
  "org.apache.curator" % "curator-recipes" % "3.0.0",
  "org.apache.curator" % "curator-framework" % "3.0.0" exclude("org.jboss.netty", "netty"),
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test->*" exclude("org.jboss.netty", "netty"),
  "org.mockito" % "mockito-core" % "1.9.5" % "test"
)

findbugsSettings

findbugsExcludeFilters := Some(scala.xml.XML.loadFile (baseDirectory.value / "findbugsExclude.xml"))

org.scalastyle.sbt.ScalastylePlugin.Settings

(testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-h", "report/squbs-zkcluster")

instrumentSettings

parallelExecution in ScoverageTest := false

parallelExecution := false
