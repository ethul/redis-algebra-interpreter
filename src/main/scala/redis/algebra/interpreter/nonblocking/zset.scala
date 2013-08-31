package redis
package algebra
package interpreter
package nonblocking

import com.redis.protocol.SortedSetCommands.{MAX, MIN, SUM}
import com.redis.serialization.ScoredValue

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.std.tuple._
import scalaz.syntax.all._
import scalaz.syntax.std.{boolean, option}, boolean._, option._

import NonBlocking._, future._

trait NonBlockingZSetInstance {
  implicit def zsetAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ZSetAlgebra] =
    new NonBlocking[ZSetAlgebra] {
      def runAlgebra[A](algebra: ZSetAlgebra[A], ops: RedisOps) =
        algebra match {
          case Zadd(k, p, h) =>
            ops.zadd(k, p.map(ScoredValue(_)).list).map(h(_))
          case Zcard(k, h) =>
            ops.zcard(k).map(h(_))
          case Zcount(k, m, n, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zcount(k, v, c, w, d).map(h(_))
          case Zincrby(k, i, m, h) =>
            ops.zincrby(k, i, m).map(a => a.cata(h(_), h(0.0)))
          case Zinterstore(d, k, None, a, h) =>
            ops.zinterstore(d, k.list, aggregate(a)).map(h(_))
          case Zinterstore(d, k, Some(w), a, h) =>
            ops.zinterstoreweighted(d, k.zip(w).list, aggregate(a)).map(h(_))
          case Zrange(k, s, t, false, h) =>
            ops.zrange(k, s.toInt, t.toInt).map(a => h(a.map((_, None))))
          case Zrange(k, s, t, true, h) =>
            ops.zrangeWithScores(k, s.toInt, t.toInt).map(a => h(a.map(_.map(_.some))))
          case Zrangebyscore(k, m, n, false, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zrangeByScore(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt))).map(a => h(a.map((_, None))))
          case Zrangebyscore(k, m, n, true, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zrangeByScoreWithScores(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt))).map(a => h(a.map(_.map(_.some))))
          case Zrank(k, m, h) =>
            ops.zrank(k, m).map(h(_))
          case Zrem(k, m, h) =>
            ops.zrem(k, m.list).map(h(_))
          case Zremrangebyrank(k, s, t, h) =>
            ops.zremrangebyrank(k, s.toInt, t.toInt).map(h(_))
          case Zremrangebyscore(k, s, t, h) =>
            val (v, _) = endpoint(s)
            val (w, _) = endpoint(t)
            ops.zremrangebyscore(k, v, w).map(h(_))
          case Zrevrange(k, s, t, false, h) =>
            ops.zrevrange(k, s.toInt, t.toInt).map(a => h(a.map((_, None))))
          case Zrevrange(k, s, t, true, h) =>
            ops.zrevrangeWithScores(k, s.toInt, t.toInt).map(a => h(a.map(_.map(_.some))))
          case Zrevrangebyscore(k, m, n, false, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zrevrangeByScore(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt))).map(a => h(a.map((_, None))))
          case Zrevrangebyscore(k, m, n, true, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zrevrangeByScoreWithScores(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt))).map(a => h(a.map(_.map(_.some))))
          case Zrevrank(k, m, h) =>
            ops.zrevrank(k, m).map(h(_))
          case Zscore(k, m, h) =>
            ops.zscore(k, m).map(h(_))
          case Zunionstore(d, k, None, a, h) =>
            ops.zunionstore(d, k.list, aggregate(a)).map(h(_))
          case Zunionstore(d, k, Some(w), a, h) =>
            ops.zunionstoreweighted(d, k.zip(w).list, aggregate(a)).map(h(_))
        }

      def endpoint(e: Endpoint) = e match {
        case Closed(v) => (v, true)
        case Open(v) => (v, false)
        case -∞ => (Double.NegativeInfinity, false)
        case +∞ => (Double.PositiveInfinity, false)
      }

      def aggregate(a: Aggregate) = a match {
        case Sum => SUM
        case Min => MIN
        case Max => MAX
      }
    }
}
