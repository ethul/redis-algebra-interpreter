package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.{ANil, ArgsOps, RedisCommand}, RedisCommand.Args
import com.redis.serialization.DefaultWriters.{anyWriter => _, _}

import akka.util.{ByteString => AkkaByteString, Timeout}

import scala.concurrent.{ExecutionContext, Future}

import scalaz.{\/, EitherT}, EitherT.eitherT
import scalaz.syntax.monad._
import scalaz.syntax.std.{boolean, option, string}, boolean._, option._, string._

import data.{Error, Get, GetResult, Host, Len, LenResult, Noone, Ok, Reset, ResetResult}, future._, syntax._

trait NonBlockingServerInstance extends ServerInstances {
  implicit def serverAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ServerAlgebra] =
    new NonBlocking[ServerAlgebra] {
      def runAlgebra[A](algebra: ServerAlgebra[A], client: RedisClient) =
        algebra match {
          case Bgrewriteaof(h) =>
            client.ask(Command[Boolean](algebra.command, ANil)).map(a => h(a.fold(Ok, Error)))
          case Bgsave(h) =>
            client.ask(Command[Boolean](algebra.command, ANil)).map(a => h(a.fold(Ok, Error)))
          case Clientgetname(h) =>
            client.ask(Command[Option[AkkaByteString]](CLIENT, GETNAME +: ANil)).map(h(_))
          case Clientkill(i, p, h) =>
            client.ask(Command[Boolean](CLIENT, KILL +: s"${i}:${p}" +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Clientlist(h) =>
            client.ask(Command[Seq[AkkaByteString]](CLIENT, LIST +: ANil)).map(h(_))
          case Clientsetname(n, h) =>
            client.ask(Command[Boolean](CLIENT, SETNAME +: n.toArray +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Configget(p, h) =>
            client.ask(Command[Seq[AkkaByteString]](CONFIG, GET +: p.toArray +: ANil)).map(h(_))
          case Configresetstat(h) =>
            client.ask(Command[Boolean](CONFIG, RESETSTAT +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Configrewrite(h) =>
            client.ask(Command[Boolean](CONFIG, REWRITE +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Configset(p, v, h) =>
            client.ask(Command[Boolean](CONFIG, SET +: p.toArray +: v.toArray +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Dbsize(h) =>
            client.ask(Command[Int](algebra.command, ANil)).map(a => h(a.toShort))
          case Debugobject(k, h) =>
            client.ask(Command[Boolean](DEBUG, OBJECT +: k.toArray +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Debugsegfault(h) =>
            client.ask(Command[Boolean](DEBUG, SEGFAULT +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Flushall(h) =>
            client.ask(Command[Boolean](algebra.command, ANil)).map(a => h(a.fold(Ok, Error)))
          case Flushdb(h) =>
            client.ask(Command[Boolean](algebra.command, ANil)).map(a => h(a.fold(Ok, Error)))
          case Info(s, h) =>
            client.ask(Command[AkkaByteString](algebra.command, s.cata(_.toArray +: ANil, ANil))).map(h(_))
          case Lastsave(h) =>
            client.ask(Command[Long](algebra.command, ANil)).map(h(_))
          case Monitor(h) =>
            Future.failed(new Exception("Unsupported operation Monitor")).map(_ => h(Stream.Empty))
          case Save(h) =>
            client.ask(Command[Boolean](algebra.command, ANil)).map(a => h(a.fold(Ok, Error)))
          case Shutdown(s, h) =>
            client.ask(Command[Boolean](algebra.command, s.cata(_.fold(SAVE, NOSAVE) +: ANil, ANil))).map(a => h(a.fold(Ok, Error)))
          case Slaveof(Noone, h) =>
            client.ask(Command[Boolean](algebra.command, NOONE +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Slaveof(Host(n, p), h) =>
            client.ask(Command[Boolean](algebra.command, n.toArray +: p +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Slowlog(Get(l), h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command, l.cata(_ +: ANil, ANil))).map(a => h(GetResult(a)))
          case Slowlog(Len, h) =>
            client.ask(Command[Int](algebra.command, ANil)).map(a => h(LenResult(a)))
          case Slowlog(Reset, h) =>
            client.ask(Command[Boolean](algebra.command, ANil)).map(_ => h(ResetResult))
          case Sync(a) =>
            client.ask(Command[Boolean](algebra.command, ANil)).map(_ => a)
          case Time(h) =>
            eitherT {
              client.ask(Command[Seq[AkkaByteString]](algebra.command, ANil)).map {
                case a :: b :: Nil =>
                  (a.utf8String.parseLong.disjunction |@| b.utf8String.parseInt.disjunction).tupled
                case a =>
                  \/.left(new NumberFormatException(s"Invalid time response: $a"))
              }
            }.map(h(_)).run.flatMap(_.fold(Future.failed(_), _.point[Future]))
        }

      val CLIENT = "CLIENT"
      val CONFIG = "CONFIG"
      val DEBUG = "DEBUG"
      val GET = "GET"
      val GETNAME = "GETNAME"
      val KILL = "KILL"
      val LIST = "LIST"
      val NOONE = "NO ONE"
      val NOSAVE = "NOSAVE"
      val OBJECT = "OBJECT"
      val RESETSTAT = "RESETSTAT"
      val REWRITE = "REWRITE"
      val SAVE = "SAVE"
      val SEGFAULT = "SEGFAULT"
      val SET = "SET"
      val SETNAME = "SETNAME"
    }
}
