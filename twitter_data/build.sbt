import Dependencies._

// Mac scalaVersion
// ThisBuild / scalaVersion := "2.12.13"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.revature"
ThisBuild / organizationName := "revature"

lazy val root = (project in file("."))
  .settings(
    name := "twitter_data",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
    // https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
    libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12",
    // https://mvnrepository.com/artifact/commons-io/commons-io
    libraryDependencies += "commons-io" % "commons-io" % "2.8.0",
    libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.427"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.