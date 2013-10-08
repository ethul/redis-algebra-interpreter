package redis
package algebra
package interpreter
package nonblocking

import org.specs2._, specification.BeforeAfter

import scalaz.NonEmptyList._
import scalaz.syntax.std.option._

import all._

/*
class NonBlockingZSetInstanceSpec extends Specification with InterpreterSpec { def is = s2"""
  This is the specification for the zset instance.

  Interpreting the zcount command with a [1, 3) range
    result in number of members that fall within that range  ${ea().e1}

  Interpreting the zcount command with a (-∞, +∞) range
    result in number of members that fall within that range  ${ea().e2}

  Interpreting the zrangebyscore command without scores
    result in the value for the given range without a score  ${eb().e3}

  Interpreting the zrangebyscore command with scores
    result in the value for the given range with a score     ${eb().e4}

  Interpreting the zrank command with an existing member
    result in a some rank of the member                      ${ec().e5}

  Interpreting the zrank command with an non-existing member
    result in a none                                         ${ec().e6}
  """

  case class ea() extends BeforeAfter {
    def before = run(zadd(key, nels((1.0, "a"), (2.0, "b"), (3.0, "c"))))

    def after = run(del(nels(key)))

    def e1 = this { run(zcount[R](key, Closed(1.0), Open(3.0))) === 2 }

    def e2 = this { run(zcount[R](key, -∞, +∞)) === 3 }

    val key = generate
  }

  case class eb() extends BeforeAfter {
    def before = run(zadd(key, nels((score, member), (2.0, "b"), (3.0, "c"))))

    def after = run(del(nels(key)))

    def e3 = this { run(zrangebyscore(key, Closed(score), Closed(2.0), false, Limit(0, 1).some)) === Seq((member, None)) }

    def e4 = this { run(zrangebyscore(key, Closed(score), Closed(2.0), true, Limit(0, 1).some)) === Seq((member, score.some)) }

    val key = generate

    val member = "a"

    val score = 1.0
  }

  case class ec() extends BeforeAfter {
    def before = run(zadd(key, nels((score, member), (2.0, "b"), (3.0, "c"))))

    def after = run(del(nels(key)))

    def e5 = this { run(zrank(key, member)) must beSome(0) }

    def e6 = this { run(zrank(key, "nonexisting")) must beNone }

    val key = generate

    val member = "a"

    val score = 1.0
  }
}
*/
