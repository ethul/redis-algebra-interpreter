package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.RedisError

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.{-\/, \/-, NonEmptyList}, NonEmptyList._
import scalaz.syntax.std.{boolean, list, option}, boolean._, list._, option._

import future._

trait NonBlockingScriptInstance extends ScriptInstances {
  implicit def scriptAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ScriptAlgebra] =
    new NonBlocking[ScriptAlgebra] {
      def runAlgebra[A](algebra: ScriptAlgebra[A], client: RedisClient) =
        algebra match {
          case Eval(s, k, a, h) =>
            client.eval(s, k, a).map(a => \/-(lua(a))).recover {
              case RedisError(a) => -\/(LuaError(a))
            }.map(h(_))
          case Evalsha(s, k, a, h) =>
            client.evalsha(s, k, a).map(a => \/-(lua(a))).recover {
              case RedisError(a) => -\/(LuaError(a))
            }.map(h(_))
          case Scriptexists(s, h) =>
            Future.sequence(s.map(a => client.script.exists(a)).list).map(a => h(a.flatten.map(_ == 1).toNel.cata(a => a, nels(false))))
          case Scriptflush(h) =>
            client.script.flush.map(a => h(a.fold(Ok, Error)))
          case Scriptkill(h) =>
            Future.failed(new Exception("Unsupported operation Scriptkill")).map(_ => h(Error))
          case Scriptload(s, h) =>
            client.script.load(s).map(h(_))
        }
    }

    def lua(exp: List[String]): LuaValue =
      exp match {
        case a :: Nil =>
          LuaString(a)
        case as =>
          LuaTable(as.map(LuaString(_)))
      }
}
