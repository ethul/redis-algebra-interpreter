package redis
package algebra
package interpreter
package nonblocking

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.boolean._

import NonBlocking._, future._

trait NonBlockingKeyInstance {
  implicit def keyAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[KeyAlgebra] =
    new NonBlocking[KeyAlgebra] {
      def runAlgebra[A](algebra: KeyAlgebra[A], ops: RedisOps) =
        algebra match {
          case Del(k, h) =>
            ops.del(k.list).map(h(_))
          case Dump(_, h) =>
            Future.failed(new Exception("Unsupported operation Dump")).map(a => h(None))
          case Exists(k, h) =>
            ops.exists(k).map(h(_))
          case Expire(k, i, h) =>
            ops.expire(k, i.toInt).map(h(_))
          case Expireat(k, a, h) =>
            ops.expireat(k, a).map(h(_))
          case Keys(p, h) =>
            ops.keys(p).map(h(_))
          case Persist(k, h) =>
            ops.persist(k).map(h(_))
          case Pexpire(k, i, h) =>
            ops.pexpire(k, i.toInt).map(h(_))
          case Pexpireat(k, a, h) =>
            ops.pexpireat(k, a).map(h(_))
          case Pttl(k, h) =>
            ops.pttl(k).map(a => h((a != -1L).option(a)))
          case Randomkey(h) =>
            ops.randomkey.map(h(_))
          case Rename(k, n, a) =>
            ops.rename(k, n).map(_ => a)
          case Renamenx(k, n, h) =>
            ops.renamenx(k, n).map(h(_))
          case Restore(k, t, v, a) =>
            Future.failed(new Exception("Unsupported operation Restore")).map(_ => a)
          case Ttl(k, h) =>
            ops.ttl(k).map(a => h((a != -1L).option(a)))
          case Type(k, h) =>
            ops.`type`(k).map {
              case "hash" => h(hash_)
              case "list" => h(list_)
              case "set" => h(set_)
              case "string" => h(string_)
              case "zset" => h(zset_)
            }
        }
    }
}
