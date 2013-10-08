package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.{-\/, \/-, Coproduct, Free}
import scalaz.syntax.all._

import NonBlocking._, future._

abstract class NonBlocking[F[_]](implicit EC: ExecutionContext, T: Timeout) {
  def runAlgebra[A](algebra: F[A], client: RedisClient): Future[A]
}

sealed trait NonBlockingInstances {
  implicit def coproductAlgebraNonBlocking[F[_]: NonBlocking, G[_]: NonBlocking]
    (implicit EC: ExecutionContext, T: Timeout): NonBlocking[({ type l[a] = Coproduct[F, G, a] })#l] =
    new NonBlocking[({ type l[a] = Coproduct[F, G, a] })#l] {
      def runAlgebra[A](algebra: ({ type l[a] = Coproduct[F, G, a] })#l[A], client: RedisClient) =
        algebra.run match {
          case -\/(fa) => implicitly[NonBlocking[F]].runAlgebra(fa, client)
          case \/-(fa) => implicitly[NonBlocking[G]].runAlgebra(fa, client)
        }
    }
}

sealed trait NonBlockingFunctions {
  def run[A](algebra: Free[R, A], client: RedisClient)(implicit EC: ExecutionContext, T: Timeout): Future[A] =
    algebra.runM(fa => implicitly[NonBlocking[R]].runAlgebra(fa, client))
}

object NonBlocking
  extends NonBlockingInstances
  with NonBlockingFunctions
  with NonBlockingConnectionInstance
  with NonBlockingHashInstance
  with NonBlockingKeyInstance
  with NonBlockingListInstance
  with NonBlockingScriptInstance
  with NonBlockingServerInstance
  with NonBlockingSetInstance
  with NonBlockingStringInstance
  with NonBlockingZSetInstance
