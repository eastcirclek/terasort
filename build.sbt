name := "terasort"

version := "0.0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"
libraryDependencies += "org.apache.flink" % "flink-scala" % "0.9.0"
libraryDependencies += "org.apache.flink" % "flink-clients" % "0.9.0"

fork in run := true

