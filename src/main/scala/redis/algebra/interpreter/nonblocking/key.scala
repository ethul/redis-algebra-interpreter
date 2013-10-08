package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.{ANil, ArgsOps, RedisCommand}, RedisCommand.Args
import com.redis.serialization.DefaultWriters.{anyWriter => _, _}

import akka.util.{ByteString => AkkaByteString, Timeout}

import scala.concurrent.{ExecutionContext, Future}

import scalaz.{-\/, \/-}
import scalaz.std.option._
import scalaz.syntax.std.{boolean, option}, boolean._, option._

import data.{By, Desc, Encoding, EncodingResult, Error, Idletime, IdletimeResult, Ok, Nosort, Pattern, Refcount, RefcountResult}
import deserializer._, future._, syntax._

trait NonBlockingKeyInstance extends KeyInstances {
  implicit def keyAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[KeyAlgebra] =
    new NonBlocking[KeyAlgebra] {
      def runAlgebra[A](algebra: KeyAlgebra[A], client: RedisClient) =
        algebra match {
          case Del(k, h) =>
            client.ask(Command[Long](algebra.command, k.list.map(_.toArray).toArgs)).map(h(_))
          case Dump(k, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, k.toArray +: ANil)).map(h(_))
          case Exists(k, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: ANil)).map(h(_))
          case Expire(k, i, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: i +: ANil)).map(h(_))
          case Expireat(k, a, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: a +: ANil)).map(h(_))
          case Keys(p, h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command, p.toArray +: ANil)).map(h(_))
          case Migrate(o, p, k, t, d, c, r, h) =>
            client.ask(Command[Boolean](algebra.command,
              o.toArray +:
              p +:
              k.toArray +:
              t +:
              d +:
              c.fold(COPY +: ANil, ANil).values ++:
              r.fold(REPLACE +: ANil, ANil)
            )).map(a => h(a.fold(Ok, Error)))
          case Move(k, d, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: d +: ANil)).map(h(_))
          case Object(s @ Refcount(k), h) =>
            client.ask(Command[Option[Long]](algebra.command, s.command +: k.toArray +: ANil)).map(a => h(a.map(RefcountResult(_))))
          case Object(s @ Encoding(k), h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, s.command +: k.toArray +: ANil)).map(a => h(a.map(EncodingResult(_))))
          case Object(s @ Idletime(k), h) =>
            client.ask(Command[Option[Long]](algebra.command, s.command +: k.toArray +: ANil)).map(a => h(a.map(IdletimeResult(_))))
          case Persist(k, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: ANil)).map(h(_))
          case Pexpire(k, i, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: i +: ANil)).map(h(_))
          case Pexpireat(k, a, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: a +: ANil)).map(h(_))
          case Pttl(k, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: ANil)).map(a => h((a != -1L).option(a)))
          case Randomkey(h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, ANil)).map(h(_))
          case Rename(k, n, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: n.toArray +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Renamenx(k, n, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: n.toArray +: ANil)).map(h(_))
          case Restore(k, t, v, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: t.cata(_ +: ANil, ANil).values ++: (v.toArray +: ANil))).map(a => h(a.fold(Ok, Error)))
          case Sort(k, b, l, g, o, a, None, h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command,
              k.toArray +:
              b.cata({ case Pattern(a) => BY +: a.toArray +: ANil case a => BY +: a.toString.toUpperCase +: ANil }, ANil).values ++:
              l.cata(a => LIMIT +: a.offset +: a.count +: ANil, ANil).values ++:
              g.map(_.toArray).toArgs.values ++:
              (o.toString.toUpperCase +: ANil).values ++:
              a.fold(ALPHA +: ANil, ANil)
            )).map(a => h(-\/(a)))
          case Sort(k, b, l, g, o, a, Some(s), h) =>
            client.ask(Command[Long](algebra.command,
              k.toArray +:
              b.cata({ case Pattern(a) => BY +: a.toArray +: ANil case a => BY +: a.toString.toUpperCase +: ANil }, ANil).values ++:
              l.cata(a => LIMIT +: a.offset +: a.count +: ANil, ANil).values ++:
              g.map(_.toArray).toArgs.values ++:
              (o.toString.toUpperCase +: ANil).values ++:
              a.fold(ALPHA +: ANil, ANil).values ++:
              s.toArray +: ANil
            )).map(a => h(\/-(a)))
          case Ttl(k, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: ANil)).map(a => h((a != -1L).option(a)))
          case Type(k, h) =>
            client.ask(Command[String](algebra.command, k.toArray +: ANil)).map {
              case "hash" => data.Hash.some
              case "list" => data.List.some
              case "set" => data.Set.some
              case "string" => data.String.some
              case "zset" => data.ZSet.some
              case _ => none
            }.map(h(_))
        }

      val COPY = "COPY"
      val REPLACE = "REPLACE"
      val BY = "BY"
      val LIMIT = "LIMIT"
      val ALPHA = "ALPHA"
    }
}
