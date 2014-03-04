scalaVersion := "2.10.3"

version in ThisBuild := "0.1.0"

publishArtifact := false

lazy val unicomplex = project

lazy val testkit = project dependsOn unicomplex
