package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.SortedSetCommands.{MAX, MIN, SUM}
import com.redis.serialization.ScoredValue

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.std.tuple._
import scalaz.syntax.all._
import scalaz.syntax.std.{boolean, option}, boolean._, option._

import future._

trait NonBlockingZSetInstance extends ZSetInstances {
  implicit def zsetAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ZSetAlgebra] =
    new NonBlocking[ZSetAlgebra] {
      def runAlgebra[A](algebra: ZSetAlgebra[A], client: RedisClient) =
        algebra match {
          case Zadd(k, p, h) =>
            client.zadd(k, p.map(ScoredValue(_)).list).map(h(_))
          case Zcard(k, h) =>
            client.zcard(k).map(h(_))
          case Zcount(k, m, n, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            client.zcount(k, v, c, w, d).map(h(_))
          case Zincrby(k, i, m, h) =>
            client.zincrby(k, i, m).map(a => a.cata(h(_), h(0.0)))
          case Zinterstore(d, k, None, a, h) =>
            client.zinterstore(d, k.list, aggregate(a)).map(h(_))
          case Zinterstore(d, k, Some(w), a, h) =>
            client.zinterstoreweighted(d, k.zip(w).list, aggregate(a)).map(h(_))
          case Zrange(k, s, t, false, h) =>
            client.zrange(k, s.toInt, t.toInt).map(a => h(a.map((_, None))))
          case Zrange(k, s, t, true, h) =>
            client.zrangeWithScores(k, s.toInt, t.toInt).map(a => h(a.map(_.map(_.some))))
          case Zrangebyscore(k, m, n, false, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            client.zrangeByScore(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt))).map(a => h(a.map((_, None))))
          case Zrangebyscore(k, m, n, true, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            client.zrangeByScoreWithScores(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt))).map(a => h(a.map(_.map(_.some))))
          case Zrank(k, m, h) =>
            client.zrank(k, m).map(h(_))
          case Zrem(k, m, h) =>
            client.zrem(k, m.list).map(h(_))
          case Zremrangebyrank(k, s, t, h) =>
            client.zremrangebyrank(k, s.toInt, t.toInt).map(h(_))
          case Zremrangebyscore(k, s, t, h) =>
            val (v, _) = endpoint(s)
            val (w, _) = endpoint(t)
            client.zremrangebyscore(k, v, w).map(h(_))
          case Zrevrange(k, s, t, false, h) =>
            client.zrevrange(k, s.toInt, t.toInt).map(a => h(a.map((_, None))))
          case Zrevrange(k, s, t, true, h) =>
            client.zrevrangeWithScores(k, s.toInt, t.toInt).map(a => h(a.map(_.map(_.some))))
          case Zrevrangebyscore(k, m, n, false, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            client.zrevrangeByScore(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt))).map(a => h(a.map((_, None))))
          case Zrevrangebyscore(k, m, n, true, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            client.zrevrangeByScoreWithScores(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt))).map(a => h(a.map(_.map(_.some))))
          case Zrevrank(k, m, h) =>
            client.zrevrank(k, m).map(h(_))
          case Zscore(k, m, h) =>
            client.zscore(k, m).map(h(_))
          case Zunionstore(d, k, None, a, h) =>
            client.zunionstore(d, k.list, aggregate(a)).map(h(_))
          case Zunionstore(d, k, Some(w), a, h) =>
            client.zunionstoreweighted(d, k.zip(w).list, aggregate(a)).map(h(_))
        }

      def endpoint(e: Endpoint) =
        e match {
          case Closed(v) => (v, true)
          case Open(v) => (v, false)
          case -∞ => (Double.NegativeInfinity, false)
          case +∞ => (Double.PositiveInfinity, false)
        }

      def aggregate(a: Aggregate) =
        a match {
          case Sum => SUM
          case Min => MIN
          case Max => MAX
        }
    }
}
