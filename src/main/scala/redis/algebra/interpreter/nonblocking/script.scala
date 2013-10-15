package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.{ANil, ArgsOps, RedisError, RedisCommand}, RedisCommand.Args
import com.redis.serialization.DefaultWriters.{anyWriter => _, _}

import akka.util.{ByteString => AkkaByteString, Timeout}

import scala.concurrent.{ExecutionContext, Future}

import scalaz.{-\/, \/-, NonEmptyList}, NonEmptyList._
import scalaz.syntax.std.{boolean, list, option}, boolean._, list._, option._

import data.{Error, LuaResult, LuaStatus, LuaString, LuaTable, Ok}, future._, syntax._

trait NonBlockingScriptInstance extends ScriptInstances {
  implicit def scriptAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ScriptAlgebra] =
    new NonBlocking[ScriptAlgebra] {
      def runAlgebra[A](algebra: ScriptAlgebra[A], client: RedisClient) =
        algebra match {
          case Eval(s, k, a, h) =>
            Future.failed(new Exception("Unsupported operation Eval")).map(_ => h(LuaStatus(Error)))
          case Evalsha(s, k, a, h) =>
            Future.failed(new Exception("Unsupported operation Evalsha")).map(_ => h(LuaStatus(Error)))
          case Scriptexists(s, h) =>
            client.ask(Command[Seq[Long]](SCRIPT, EXISTS +: s.map(_.toArray).list.toArgs)).map(a => h(a.map(_ == 1).toList.toNel.cata(a => a, nels(false))))
          case Scriptflush(h) =>
            client.ask(Command[Boolean](SCRIPT, FLUSH +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Scriptkill(h) =>
            client.ask(Command[Boolean](SCRIPT, KILL +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Scriptload(s, h) =>
            client.ask(Command[Option[AkkaByteString]](SCRIPT, LOAD +: s.toArray +: ANil)).map(h(_))
        }

      val SCRIPT = "SCRIPT"
      val FLUSH = "FLUSH"
      val KILL = "KILL"
      val LOAD = "LOAD"
      val EXISTS = "EXISTS"
    }
}
