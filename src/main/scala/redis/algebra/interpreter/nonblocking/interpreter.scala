package redis
package algebra
package interpreter
package nonblocking

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.{-\/, \/-, Coproduct, Free, Functor}
import scalaz.syntax.all._

import NonBlocking._, future._

abstract class NonBlocking[F[_] : Functor](implicit EC: ExecutionContext, T: Timeout) {
  def runAlgebra[A](algebra: F[A], ops: RedisOps): Future[A]
}

sealed trait NonBlockingInstances {
  implicit def coproductAlgebraNonBlocking[F[_]: NonBlocking : Functor, G[_]: NonBlocking : Functor]
    (implicit EC: ExecutionContext, T: Timeout): NonBlocking[({ type l[a] = Coproduct[F, G, a] })#l] =
    new NonBlocking[({ type l[a] = Coproduct[F, G, a] })#l] {
      def runAlgebra[A](algebra: ({ type l[a] = Coproduct[F, G, a] })#l[A], ops: RedisOps) =
        algebra.run match {
          case -\/(fa) => implicitly[NonBlocking[F]].runAlgebra(fa, ops)
          case \/-(fa) => implicitly[NonBlocking[G]].runAlgebra(fa, ops)
        }
    }
}

sealed trait NonBlockingFunctions {
  def run[A](algebra: Free[R, A], ops: RedisOps)
    (implicit EC: ExecutionContext, T: Timeout): Future[A] =
    algebra.resume.fold({ (a: R[Free[R, A]]) =>
      implicitly[NonBlocking[R]].runAlgebra(a, ops).flatMap { a =>
        run(a, ops)
      }
    }, a => a.point[Future])
}

sealed trait NonBlockingTypes {
  import com.redis.api._

  type RedisOps = StringOperations
    with ListOperations
    with SetOperations
    with SortedSetOperations
    with HashOperations
    with KeyOperations
    with NodeOperations
    with EvalOperations
}

object NonBlocking
  extends NonBlockingInstances
  with NonBlockingKeyInstance
  with NonBlockingHashInstance
  with NonBlockingListInstance
  with NonBlockingSetInstance
  with NonBlockingStringInstance
  with NonBlockingZSetInstance
  with NonBlockingFunctions
  with NonBlockingTypes
