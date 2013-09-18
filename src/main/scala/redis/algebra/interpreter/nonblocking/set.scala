package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.option._

import future._

trait NonBlockingSetInstance extends SetInstances {
  implicit def setAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[SetAlgebra] =
    new NonBlocking[SetAlgebra] {
      def runAlgebra[A](algebra: SetAlgebra[A], client: RedisClient) =
        algebra match {
          case Sadd(k, m, h) =>
            client.sadd(k, m.head, m.tail:_*).map(h(_))
          case Scard(k, h) =>
            client.scard(k).map(h(_))
          case Sdiff(k, h) =>
            client.sdiff(k.head, k.tail:_*).map(h(_))
          case Sdiffstore(d, k, h) =>
            client.sdiffstore(d, k.head, k.tail:_*).map(h(_))
          case Sinter(k, h) =>
            client.sinter(k.head, k.tail:_*).map(h(_))
          case Sinterstore(d, k, h) =>
            client.sinterstore(d, k.head, k.tail:_*).map(h(_))
          case Sismember(k, m, h) =>
            client.sismember(k, m).map(h(_))
          case Smembers(k, h) =>
            client.smembers(k).map(h(_))
          case Smove(s, d, m, h) =>
            client.smove(s, d, m).map(a => h(a == 1L))
          case Spop(k, h) =>
            client.spop(k).map(h(_))
          case Srandmember(k, c, h) =>
            client.srandmember(k, c.cata(a => a.toInt, 1)).map(a => h(a.toSet))
          case Srem(k, m, h) =>
            client.srem(k, m.head, m.tail:_*).map(h(_))
          case Sunion(k, h) =>
            client.sunion(k.head, k.tail:_*).map(h(_))
          case Sunionstore(d, k, h) =>
            client.sunionstore(d, k.head, k.tail:_*).map(h(_))
        }
    }
}
