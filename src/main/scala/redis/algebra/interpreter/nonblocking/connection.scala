package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.boolean._

import future._

trait NonBlockingConnectionInstance extends ConnectionInstances {
  implicit def connectionAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ConnectionAlgebra] =
    new NonBlocking[ConnectionAlgebra] {
      def runAlgebra[A](algebra: ConnectionAlgebra[A], client: RedisClient) =
        algebra match {
          case Auth(p, h) =>
            client.auth(p).map(a => h(a.fold(Ok, Error)))
          case Echo(m, h) =>
            Future.failed(new Exception("Unsupported operation Echo")).map(_ => h(""))
          case Ping(h) =>
            Future.failed(new Exception("Unsupported operation Ping")).map(_ => h(Error))
          case Quit(h) =>
            client.quit.map(a => h(a.fold(Ok, Error)))
          case Select(i, h) =>
            client.select(i).map(a => h(a.fold(Ok, Error)))
        }
    }
}
