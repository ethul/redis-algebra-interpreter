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

trait NonBlockingListInstance extends ListInstances {
  implicit def listAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ListAlgebra] =
    new NonBlocking[ListAlgebra] {
      def runAlgebra[A](algebra: ListAlgebra[A], client: RedisClient) =
        algebra match {
          case Blpop(k, t, h) =>
            client.ask(Command[Option[(AkkaByteString, AkkaByteString)]](algebra.command, t +: k.list.map(_.toArray).toArgs)).map(h(_))
          case Brpop(k, t, h) =>
            client.ask(Command[Option[(AkkaByteString, AkkaByteString)]](algebra.command, t +: k.list.map(_.toArray).toArgs)).map(h(_))
          case Brpoplpush(s, d, t, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, s.toArray +: d.toArray +: t +: ANil)).map(h(_))
          case Lindex(k, i, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, k.toArray +: i +: ANil)).map(h(_))
          case Linsert(k, o, p, v, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: o.toString.toUpperCase +: p.toArray +: v.toArray +: ANil)).map(a => h((a != -1L).option(a)))
          case Llen(k, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: ANil)).map(h(_))
          case Lpop(k, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, k.toArray +: ANil)).map(h(_))
          case Lpush(k, v, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: v.head.toArray +: v.tail.map(_.toArray).toArgs)).map(h(_))
          case Lpushx(k, v, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: v.toArray +: ANil)).map(h(_))
          case Lrange(k, s, t, h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command, k.toArray +: s +: t +: ANil)).map(h(_))
          case Lrem(k, c, v, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: c +: v.toArray +: ANil)).map(h(_))
          case Lset(k, i, v, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: i +: v.toArray +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Ltrim(k, s, t, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: s +: t +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Rpop(k, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, k.toArray +: ANil)).map(h(_))
          case Rpoplpush(s, d, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, s.toArray +: d.toArray +: ANil)).map(h(_))
          case Rpush(k, v, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: v.map(_.toArray).list.toArgs)).map(h(_))
          case Rpushx(k, v, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: v.toArray +: ANil)).map(h(_))
        }
    }
}
