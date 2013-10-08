package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.{ANil, ArgsOps, RedisCommand}, RedisCommand.Args
import com.redis.serialization.DefaultWriters.{anyWriter => _, _}

import akka.util.{ByteString => AkkaByteString, Timeout}

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.boolean._

import data.{Error, Ok}, future._, syntax._

trait NonBlockingConnectionInstance extends ConnectionInstances {
  implicit def connectionAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ConnectionAlgebra] =
    new NonBlocking[ConnectionAlgebra] {
      def runAlgebra[A](algebra: ConnectionAlgebra[A], client: RedisClient) =
        algebra match {
          case Auth(p, h) =>
            client.ask(Command[Boolean](algebra.command, p.toArray +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Echo(m, h) =>
            client.ask(Command[AkkaByteString](algebra.command, m.toArray +: ANil)).map(h(_))
          case Ping(h) =>
            client.ask(Command[Boolean](algebra.command, ANil)).map(a => h(a.fold(Ok, Error)))
          case Quit(h) =>
            client.ask(Command[Boolean](algebra.command, ANil)).map(a => h(a.fold(Ok, Error)))
          case Select(i, h) =>
            client.ask(Command[Boolean](algebra.command, i +: ANil)).map(a => h(a.fold(Ok, Error)))
        }
    }
}
