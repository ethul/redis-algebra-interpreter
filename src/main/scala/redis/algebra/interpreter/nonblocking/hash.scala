package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.boolean._

import future._

trait NonBlockingHashInstance extends HashInstances {
  implicit def hashAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[HashAlgebra] =
    new NonBlocking[HashAlgebra] {
      def runAlgebra[A](algebra: HashAlgebra[A], client: RedisClient) =
        algebra match {
          case Hdel(k, f, h) =>
            client.hdel(k, f.list).map(h(_))
          case Hexists(k, f, h) =>
            client.hexists(k, f).map(h(_))
          case Hget(k, f, h) =>
            client.hget(k, f).map(h(_))
          case Hgetall(k, h) =>
            client.hgetall(k).map(a => h(a.toList))
          case Hincrby(k, f, i, h) =>
            client.hincrby(k, f, i.toInt).map(h(_))
          case Hincrbyfloat(k, f, i, h) =>
            Future.failed(new Exception("Unsupported operation Hincrbyfloat")).map(_ => h(0))
          case Hkeys(k, h) =>
            client.hkeys(k).map(h(_))
          case Hlen(k, h) =>
            client.hlen(k).map(h(_))
          case Hmget(k, f, h) =>
            client.hmget[String](k, f.list).map(a => h(f.map(a.get(_).flatMap(a => (a != null).option(a))).list))
          case Hmset(k, p, h) =>
            client.hmset(k, p.list).map(a => h(a.fold(Ok, Error)))
          case Hset(k, f, v, h) =>
            client.hset(k, f, v).map(h(_))
          case Hsetnx(k, f, v, h) =>
            client.hsetnx(k, f, v).map(h(_))
          case Hvals(k, h) =>
            client.hvals(k).map(h(_))
        }
    }
}
