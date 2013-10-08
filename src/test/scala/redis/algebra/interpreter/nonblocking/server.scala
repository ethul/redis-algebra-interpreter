package redis
package algebra
package interpreter
package nonblocking

import org.specs2._

import all._

/*
class NonBlockingServerInstanceSpec extends Specification with InterpreterSpec { def is = s2"""
  This is the specification for the server instance.

  Interpreting the clientlist command should
    result in a map containing pairs of the specific client values        ${ea().e1}

  Interpreting the configget command should
    result in a sequence containing the config values                     ${ea().e2}

  Interpreting the info command should
    result in a map containing maps of each section                       ${ea().e3}
  """

  case class ea() {
    def e1 = run(clientlist) must beLike {
      case a :: as =>
        a must haveKeys(
          "addr", "fd", "name", "age", "idle", "flags", "db", "sub", "psub",
          "multi", "qbuf", "qbuf-free", "obl", "oll", "omem", "events", "cmd"
        )
    }

    def e2 = run(configget("hash-max-ziplist-entries")) must beLike {
      case a :: b :: as =>
        (a === "hash-max-ziplist-entries") and (b must beMatching("""\d+"""))
    }

    def e3 = run(info()) must haveKeys("a")
  }
}
*/
