package redis
package algebra
package interpreter
package nonblocking

import scala.concurrent.{ExecutionContext, Future}

import scalaz.Monad

sealed trait FutureInstances {
  implicit def futureMonad(implicit EC: ExecutionContext): Monad[Future] =
    new Monad[Future] {
      override def map[A, B](fa: Future[A])(f: A => B): Future[B] = fa.map(f)
      def point[A](a: => A): Future[A] = Future(a)
      def bind[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
    }
}

object future extends FutureInstances
