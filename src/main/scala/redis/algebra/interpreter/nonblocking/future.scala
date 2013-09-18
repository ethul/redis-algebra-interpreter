package redis
package algebra
package interpreter
package nonblocking

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import scalaz.{Comonad, Monad}

sealed trait FutureInstances {
  implicit def futureMonad(implicit EC: ExecutionContext): Monad[Future] =
    new Monad[Future] {
      override def map[A, B](fa: Future[A])(f: A => B): Future[B] = fa.map(f)
      def point[A](a: => A): Future[A] = Future(a)
      def bind[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
    }

  implicit def futureComonad(implicit EC: ExecutionContext, D: Duration): Comonad[Future] =
    new Comonad[Future] {
      override def map[A, B](fa: Future[A])(f: A => B): Future[B] = futureMonad.map(fa)(f)
      def copoint[A](fa: Future[A]): A = Await.result(fa, D)
      def cobind[A, B](fa: Future[A])(f: Future[A] => B): Future[B] = Future(f(fa))
    }
}

object future extends FutureInstances
