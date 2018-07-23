import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbt.CrossVersion

val akkaVersion = "2.5.14"

val `crdt-recipes` = project
  .in(file("."))
  .settings(SbtMultiJvm.multiJvmSettings: _*)
  .settings(
    name := "crdt-recipes",
    version := "1.0.0",
    scalaVersion := "2.12.6",

    scalacOptions in Compile ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),

    javacOptions in Compile ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),

    javaOptions in run ++= Seq("-Xms128m", "-Xmx1024m"),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion,

      "eu.timepit" %% "crjdt-core" % "0.0.8-SNAPSHOT", //local build

      "com.twitter" %% "algebird-core" % "0.13.0",

      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.3",

      "com.github.mpilquist" %% "simulacrum" % "0.12.0",

      "org.rocksdb" % "rocksdbjni" % "5.5.1",
      "io.dmitryivanov" %% "scala-crdt" % "1.0.1",

      //"com.rbmhtechnology" %% "eventuate-crdt" % "0.10",

      "Merlijn Boogerd" %% "computational-crdts" % "1.0",

      "com.github.mpilquist" %% "simulacrum" % "0.12.0",
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion
    ),

    fork in run := true,

    // disable parallel tests
    parallelExecution in Test := false

  ) configs (MultiJvm)


/*
val project = Project(
  id = "crdt-recipes",
  base = file("."),
  settings = Defaults.coreDefaultSettings ++ SbtMultiJvm.multiJvmSettings ++ Seq(
    name := "crdt-recipes",
    version := "1.0.0",
    scalaVersion := "2.12.6",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,

      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion,

      "eu.timepit" %% "crjdt-core" % "0.0.8-SNAPSHOT", //local build

      "com.twitter" %% "algebird-core" % "0.13.0",

      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.3",

      "com.github.mpilquist" %% "simulacrum" % "0.12.0",

      "org.rocksdb" % "rocksdbjni" % "5.5.1",
      "io.dmitryivanov" %% "scala-crdt" % "1.0.1",

      //"com.rbmhtechnology" %% "eventuate-crdt" % "0.10",

      "Merlijn Boogerd" %% "computational-crdts" % "1.0",

      "com.github.mpilquist" %% "simulacrum" % "0.12.0",
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion
    ),
    // make sure that MultiJvm test are compiled by the default test compilation
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    // disable parallel tests
    parallelExecution in Test := false,
    // make sure that MultiJvm tests are executed by the default test target,
    // and combine the results from ordinary test and multi-jvm tests
    executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
      case (testResults, multiNodeResults) =>
        val overall =
          if (testResults.overall.id < multiNodeResults.overall.id)
            multiNodeResults.overall
          else
            testResults.overall
        Tests.Output(overall,
          testResults.events ++ multiNodeResults.events,
          testResults.summaries ++ multiNodeResults.summaries)
    }
  )
) configs (MultiJvm)
*/


//https://tpolecat.github.io/2017/04/25/scalac-flags.html

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

//"com.rbmhtechnology" %% "eventuate-crdt"  % "0.9"
//com.rbmhtechnology:eventuate-crdt_2.12:0.9         (depends on 2.4.12)
