name := "spark-project-for-calc"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

val circeVersion = "0.8.0"

ThisBuild / useCoursier := false

libraryDependencies ++= Seq(
  "org.apache.spark"        %% "spark-core"           % sparkVersion,
  "org.apache.spark"        %% "spark-sql"            % sparkVersion,
  "org.apache.spark"        %% "spark-mllib"          % sparkVersion,
  "org.apache.spark"        %% "spark-hive"           % sparkVersion,
  "com.github.fommil.netlib" % "all"                  % "1.1.2" pomOnly (),
  "com.github.seratch"      %% "awscala"              % "0.6.2",
  "com.github.nscala-time"  %% "nscala-time"          % "2.16.0",
  "org.apache.hadoop"        % "hadoop-aws"           % "2.8.1",
  "io.circe"                %% "circe-core"           % circeVersion,
  "io.circe"                %% "circe-generic"        % circeVersion,
  "io.circe"                %% "circe-parser"         % circeVersion,
  "com.amazonaws"            % "aws-java-sdk-emr"     % "1.11.272",
  "com.typesafe"             % "config"               % "1.3.1",
  "org.apache.logging.log4j" % "log4j-api"            % "2.17.1",
  "org.apache.logging.log4j" % "log4j-core"           % "2.17.1",
  "org.apache.logging.log4j" % "log4j-api-scala_2.11" % "11.0",
  "net.snowflake"            % "snowflake-jdbc"       % "3.8.0",
  "net.snowflake"           %% "spark-snowflake"      % "2.4.14-spark_2.4",
  "com.redislabs"            % "spark-redis"          % "2.4.0",
  "com.thesamet.scalapb"    %% "sparksql-scalapb"     % "0.9.0",
  "mysql"                    % "mysql-connector-java" % "5.1.36",
  "org.scalatest"           %% "scalatest"            % "3.2.2" % "test",
  "com.typesafe.akka"       %% "akka-http"            % "10.0.10",
  "com.google.code.gson"     % "gson"                 % "2.4"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)               => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".xml"        => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html"       => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".types"      => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class"      => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".thrift"     => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".txt"        => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".fmpp"       => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll
)

assemblyJarName in assembly := { "spark-projectForCalc.jar" }

scalacOptions in (Compile, doc) := Seq("-groups", "-implicits", "-diagrams")

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value / "scalapb"
)
