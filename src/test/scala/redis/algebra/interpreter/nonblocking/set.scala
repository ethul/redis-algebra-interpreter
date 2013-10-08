package redis
package algebra
package interpreter
package nonblocking

import org.specs2._, specification.BeforeAfter

import scalaz.NonEmptyList._
import scalaz.syntax.std.option._

import all._

/*
class NonBlockingSetInstanceSpec extends Specification with InterpreterSpec { def is = s2"""
  This is the specification for the set instance.

  Interpreting the smove command when the element has been moved should
    result in true value                                                     ${ea().e1}

  Interpreting the smove command when the element has not been moved should
    result in false value                                                    ${ea().e2}
  """

  case class ea() extends BeforeAfter {
    def before = run(sadd(key1, nels(member)))

    def after = run(del(nels(key1)))

    def e1 = this { run(smove(key1, key2, member)) === true }

    def e2 = this { run(smove(key1, key2, "nonexisting")) === false }

    val (key1, key2) = (generate, generate)

    val member = generate
  }
}
*/
