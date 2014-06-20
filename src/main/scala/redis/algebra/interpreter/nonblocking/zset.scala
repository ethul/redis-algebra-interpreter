package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.{ANil, ArgsOps, RedisCommand}, RedisCommand.Args
import com.redis.serialization.DefaultWriters.{anyWriter => _, _}

import akka.util.{ByteString => AkkaByteString, Timeout}

import scala.collection.immutable.{Set => ScalaSet}
import scala.concurrent.{ExecutionContext, Future}

import scalaz.{\/, EitherT}, EitherT.eitherT
import scalaz.std.list._
import scalaz.std.tuple._
import scalaz.syntax.all._
import scalaz.syntax.std.{boolean, option, string}, boolean._, option._, string._

import data.{-∞, +∞, Aggregate, Closed, Endpoint, Max, Min, Open, Sum}, deserializer._, future._, syntax._

trait NonBlockingZSetInstance extends ZSetInstances {
  implicit def zsetAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ZSetAlgebra] =
    new NonBlocking[ZSetAlgebra] {
      def runAlgebra[A](algebra: ZSetAlgebra[A], client: RedisClient) =
        algebra match {
          case Zadd(k, p, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: p.foldLeft(ANil)((b,a) => a._1 +: a._2.toArray +: b))).map(h(_))
          case Zcard(k, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: ANil)).map(h(_))
          case Zcount(k, m, n, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: endpoint(m) +: endpoint(n) +: ANil)).map(h(_))
          case Zincrby(k, i, m, h) =>
            eitherT(client.ask(Command[AkkaByteString](algebra.command, k.toArray +: i +: m.toArray +: ANil)).map(_.utf8String.parseDouble.disjunction)).
              map(h(_)).run.flatMap(_.fold(Future.failed(_), _.point[Future]))
          case Zinterstore(d, k, w, a, h) =>
            client.ask(Command[Long](algebra.command,
              d.toArray +:
              k.size +:
              k.map(_.toArray).list.toArgs.values ++:
              w.cata(_.foldLeft(ANil)((b,a) => a +: b), ANil).values ++:
              (a.toString.toUpperCase +: ANil)
            )).map(h(_))
          case Zrange(k, s, t, false, h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command, k.toArray +: s +: t +: ANil)).map(a => h(a.map((_, None))))
          case Zrange(k, s, t, true, h) =>
            eitherT {
              client.ask(Command[Seq[Seq[AkkaByteString]]](algebra.command, k.toArray +: s +: t +: WITHSCORES +: ANil)).map { as =>
                as.map {
                  case a :: b :: Nil =>
                    b.utf8String.parseDouble.map((a, _)).disjunction
                  case a =>
                    \/.left(new NumberFormatException(s"Invalid scores: $a"))
                }.toList.sequenceU
              }
            }.map(a => h(a.map(_.bimap(a => a, _.some)))).run.flatMap(_.fold(Future.failed(_), _.point[Future]))
          case Zrangebyscore(k, m, n, false, l, h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command,
              k.toArray +:
              endpoint(m) +:
              endpoint(n) +:
              l.cata(a => LIMIT +: a.offset +: a.count +: ANil, ANil)
            )).map(a => h(a.map((_, None))))
          case Zrangebyscore(k, m, n, true, l, h) =>
            eitherT {
              client.ask(Command[Seq[Seq[AkkaByteString]]](algebra.command,
                k.toArray +:
                endpoint(m) +:
                endpoint(n) +:
                WITHSCORES +:
                l.cata(a => LIMIT +: a.offset +: a.count +: ANil, ANil)
              )).map { as =>
                as.map {
                  case a :: b :: Nil =>
                    b.utf8String.parseDouble.map((a, _)).disjunction
                  case a =>
                    \/.left(new NumberFormatException(s"Invalid scores: $a"))
                }.toList.sequenceU
              }
            }.map(a => h(a.map(_.bimap(a => a, _.some)))).run.flatMap(_.fold(Future.failed(_), _.point[Future]))
          case Zrank(k, m, h) =>
            client.ask(Command[Option[Long]](algebra.command, k.toArray +: m.toArray +: ANil)).map(h(_))
          case Zrem(k, m, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: m.map(_.toArray).list.toArgs)).map(h(_))
          case Zremrangebyrank(k, s, t, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: s +: t +: ANil)).map(h(_))
          case Zremrangebyscore(k, s, t, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: endpoint(s) +: endpoint(t) +: ANil)).map(h(_))
          case Zrevrange(k, s, t, false, h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command, k.toArray +: s +: t +: ANil)).map(a => h(a.map((_, None))))
          case Zrevrange(k, s, t, true, h) =>
            eitherT {
              client.ask(Command[Seq[Seq[AkkaByteString]]](algebra.command, k.toArray +: s +: t +: WITHSCORES +: ANil)).map { as =>
                as.map {
                  case a :: b :: Nil =>
                    b.utf8String.parseDouble.map((a, _)).disjunction
                  case a =>
                    \/.left(new NumberFormatException(s"Invalid scores: $a"))
                }.toList.sequenceU
              }
            }.map(a => h(a.map(_.bimap(a => a, _.some)))).run.flatMap(_.fold(Future.failed(_), _.point[Future]))
          case Zrevrangebyscore(k, m, n, false, l, h) =>
            client.ask(Command[Seq[AkkaByteString]](algebra.command,
              k.toArray +:
              endpoint(m) +:
              endpoint(n) +:
              l.cata(a => LIMIT +: a.offset +: a.count +: ANil, ANil)
            )).map(a => h(a.map((_, None))))
          case Zrevrangebyscore(k, m, n, true, l, h) =>
            eitherT {
              client.ask(Command[Seq[Seq[AkkaByteString]]](algebra.command,
                k.toArray +:
                endpoint(m) +:
                endpoint(n) +:
                WITHSCORES +:
                l.cata(a => LIMIT +: a.offset +: a.count +: ANil, ANil)
              )).map { as =>
                as.map {
                  case a :: b :: Nil =>
                    b.utf8String.parseDouble.map((a, _)).disjunction
                  case a =>
                    \/.left(new NumberFormatException(s"Invalid scores: $a"))
                }.toList.sequenceU
              }
            }.map(a => h(a.map(_.bimap(a => a, _.some)))).run.flatMap(_.fold(Future.failed(_), _.point[Future]))
          case Zrevrank(k, m, h) =>
            client.ask(Command[Option[Long]](algebra.command, k.toArray +: m.toArray +: ANil)).map(h(_))
          case Zscore(k, m, h) =>
            client.ask(Command[Option[Double]](algebra.command, k.toArray +: m.toArray +: ANil)).map(h(_))
          case Zunionstore(d, k, w, a, h) =>
            client.ask(Command[Long](algebra.command,
              d.toArray +:
              k.size +:
              k.map(_.toArray).list.toArgs.values ++:
              w.cata(_.foldLeft(ANil)((b,a) => a +: b), ANil).values ++:
              (a.toString.toUpperCase +: ANil)
            )).map(h(_))
        }

      def endpoint(e: Endpoint) =
        e match {
          case Closed(v) => s"$v"
          case Open(v) => s"($v"
          case -∞ => "-inf"
          case +∞ => "_inf"
        }

      val LIMIT = "LIMIT"
      val WITHSCORES = "WITHSCORES"
    }
}
