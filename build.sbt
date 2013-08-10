name := "redis-algebra-interpreter"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies += "redis-algebra" %% "redis-algebra" % "0.0.1-SNAPSHOT"

libraryDependencies += "net.debasishg" %% "redisclientnb" % "2.10"

resolvers += "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"

resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

//resolvers += "Ethul repository snapshots" at "https://github.com/ethul/ivy-repository/raw/master/snapshots/"

resolvers += "Ethul repository snapshots" at "file://"+Path.userHome.absolutePath+"/tmp/scala/ivy-repo/snapshots/"

resolvers += "Ethul repository releases" at "https://github.com/ethul/ivy-repository/raw/master/releases/"

scalacOptions += "-feature"

scalacOptions += "-language:higherKinds"

scalacOptions += "-language:implicitConversions"

scalacOptions += "-Xlint"

scalacOptions += "-Xfatal-warnings"

scalacOptions += "-Yno-adapted-args"

scalacOptions += "-Ywarn-all"
