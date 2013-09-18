package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.{-\/, \/-}
import scalaz.syntax.std.{boolean, option}, boolean._, option._

import future._

trait NonBlockingKeyInstance extends KeyInstances {
  implicit def keyAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[KeyAlgebra] =
    new NonBlocking[KeyAlgebra] {
      def runAlgebra[A](algebra: KeyAlgebra[A], client: RedisClient) =
        algebra match {
          case Del(k, h) =>
            client.del(k.list).map(h(_))
          case Dump(_, h) =>
            Future.failed(new Exception("Unsupported operation Dump")).map(a => h(None))
          case Exists(k, h) =>
            client.exists(k).map(h(_))
          case Expire(k, i, h) =>
            client.expire(k, i.toInt).map(h(_))
          case Expireat(k, a, h) =>
            client.expireat(k, a).map(h(_))
          case Keys(p, h) =>
            client.keys(p).map(h(_))
          case Migrate(_, _, _, _, _, _, _, h) =>
            Future.failed(new Exception("Unsupported operation Migrate")).map(a => h(Error))
          case Move(k, d, h) =>
            client.move(k, d).map(h(_))
          case Object(_, h) =>
            Future.failed(new Exception("Unsupported operation Object")).map(a => h(None))
          case Persist(k, h) =>
            client.persist(k).map(h(_))
          case Pexpire(k, i, h) =>
            client.pexpire(k, i.toInt).map(h(_))
          case Pexpireat(k, a, h) =>
            client.pexpireat(k, a).map(h(_))
          case Pttl(k, h) =>
            client.pttl(k).map(a => h((a != -1L).option(a)))
          case Randomkey(h) =>
            client.randomkey.map(h(_))
          case Rename(k, n, h) =>
            client.rename(k, n).map(a => h(a.fold(Ok, Error)))
          case Renamenx(k, n, h) =>
            client.renamenx(k, n).map(h(_))
          case Restore(k, t, v, h) =>
            Future.failed(new Exception("Unsupported operation Restore")).map(_ => h(Error))
          case Sort(k, b, l, g, o, a, None, h) =>
            client.sort(k, l.map(a => (a.offset.toInt, a.count.toInt)), o == Desc, a, b.map(by(_)), g).map(a => h(-\/(a)))
          case Sort(k, b, l, g, o, a, Some(s), h) =>
            client.sortNStore(k, l.map(a => (a.offset.toInt, a.count.toInt)), o == Desc, a, b.map(by(_)), g, s).map(a => h(\/-(a)))
          case Ttl(k, h) =>
            client.ttl(k).map(a => h((a != -1L).option(a)))
          case Type(k, h) =>
            client.`type`(k).map {
              case "hash" => h(RedisHash.some)
              case "list" => h(RedisList.some)
              case "set" => h(RedisSet.some)
              case "string" => h(RedisString.some)
              case "zset" => h(RedisZSet.some)
              case _ => h(None)
            }
        }

      def by(a: By) =
        a match {
          case Nosort => "NOSORT"
          case Pattern(a) => a
        }
    }
}
