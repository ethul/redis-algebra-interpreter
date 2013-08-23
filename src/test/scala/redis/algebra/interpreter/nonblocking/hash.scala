package redis
package algebra
package interpreter
package nonblocking

import org.specs2._, specification.BeforeAfter

import scalaz.NonEmptyList._
import scalaz.syntax.std.option._

import HashAlgebra._, KeyAlgebra._

class NonBlockingHashInstanceSpec extends Specification with InterpreterSpec { def is = s2"""
  This is the specification for the hash instance.

  Interpreting the hmget command when one of the keys does not exist
    result in a list with some wrapping existing values and none otherwise   ${ea().e1}
  """

  case class ea() extends BeforeAfter {
    def before = run(hmset(key, nels((field1, value1), (field2, value2), (field3, value3))))

    def after = run(del(nels(key)))

    def e1 = this { run(hmget(key, nels(field1, s"${field2}a", field3))) === List(value1.some, None, value3.some) }

    val key = generate

    val (field1, field2, field3) = (generate, generate, generate)

    val (value1, value2, value3) = ("a", "b", "c")
  }
}
