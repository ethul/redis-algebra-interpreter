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

trait NonBlockingHashInstance extends HashInstances {
  implicit def hashAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[HashAlgebra] =
    new NonBlocking[HashAlgebra] {
      def runAlgebra[A](algebra: HashAlgebra[A], client: RedisClient) =
        algebra match {
          case Hdel(k, f, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: f.map(_.toArray).list.toArgs)).map(h(_))
          case Hexists(k, f, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: f.toArray +: ANil)).map(h(_))
          case Hget(k, f, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, k.toArray +: f.toArray +: ANil)).map(h(_))
          case Hgetall(k, h) =>
            client.ask(Command[Map[AkkaByteString, AkkaByteString]](algebra.command, k.toArray +: ANil)).map(a => h(a.toList))
          case Hincrby(k, f, i, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: f.toArray +: i +: ANil)).map(h(_))
          case Hincrbyfloat(k, f, i, h) =>
            client.ask(Command[String](algebra.command, k.toArray +: f.toArray +: i.toString +: ANil)).map(a => h(BigDecimal(a)))
          case Hkeys(k, h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command, k.toArray +: ANil)).map(h(_))
          case Hlen(k, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: ANil)).map(h(_))
          case Hmget(k, f, h) =>
            client.ask(Command[Seq[Option[AkkaByteString]]](algebra.command, k.toArray +: f.list.map(_.toArray).toArgs)).map(h(_))
          case Hmset(k, p, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: p.map{case (a,b) => (a ++ b).toArray}.list.toArgs)).map(a => h(a.fold(Ok, Error)))
          case Hset(k, f, v, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: f.toArray +: v.toArray +: ANil)).map(h(_))
          case Hsetnx(k, f, v, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: f.toArray +: v.toArray +: ANil)).map(h(_))
          case Hvals(k, h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command, k.toArray +: ANil)).map(h(_))
        }
    }
}
