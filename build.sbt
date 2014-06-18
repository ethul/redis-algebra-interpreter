organization := "com.github.ethul"

name := "redis-algebra-interpreter"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.4"

libraryDependencies += "com.github.ethul" %% "redis-algebra" % "0.1.0-SNAPSHOT"

libraryDependencies += "net.debasishg" %% "redisreact" % "0.5"

libraryDependencies += "org.specs2" %% "specs2" % "2.2.2-scalaz-7.1.0-M3" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"

resolvers += "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

scalacOptions += "-feature"

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"

scalacOptions += "-language:higherKinds"

scalacOptions += "-language:implicitConversions"

scalacOptions += "-Xlint"

scalacOptions += "-Xfatal-warnings"

scalacOptions += "-Yno-adapted-args"

scalacOptions += "-Ywarn-all"

publishTo <<= version.apply { v =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("Snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("Releases" at nexus + "service/local/staging/deploy/maven2")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

pomIncludeRepository := Function.const(false)

pomExtra :=
  <licenses>
    <license>
      <name>MIT</name>
      <url>http://www.opensource.org/licenses/mit-license.php</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>https://github.com/ethul/redis-algebra-interpreter</url>
    <connection>scm:git:git@github.com:ethul/redis-algebra-interpreter.git</connection>
    <developerConnection>scm:git:git@github.com:ethul/redis-algebra-interpreter.git</developerConnection>
  </scm>
  <developers>
    <developer>
      <id>ethul</id>
      <name>Eric Thul</name>
      <url>https://github.com/ethul</url>
    </developer>
  </developers>
