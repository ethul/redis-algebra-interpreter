package redis
package algebra
package interpreter
package nonblocking

import org.specs2._

import scalaz.{-\/, \/-}

import all._

class NonBlockingScriptInstanceSpec extends Specification with InterpreterSpec { def is = s2"""
  This is the specification for the script instance.

  Interpreting the eval command with a script with an error should
    result in a LuaError                                                  ${ea().e1}

  Interpreting the eval command with a script returning a string should
    result in a LuaString                                                 ${ea().e2}

  Interpreting the eval command returning a string list should
    result in a LuaTable of LuaString values                              ${ea().e3}
  """

  case class ea() {
    def e1 = run(eval(s"""return {err = "$error"}""")) must beLike {
      case -\/(LuaError(a)) =>
        a === error
      case _ =>
        ko
    }

    def e2 = run(eval(s"""return "$string"""")) must beLike {
      case \/-(LuaString(a)) =>
        a === string
      case _ =>
        ko
    }

    def e3 = run(eval(s"""return {"$string", "$string", "$string"}""")) must beLike {
      case \/-(LuaTable(LuaString(a) :: LuaString(b) :: LuaString(c) :: Nil)) =>
        a === string and b === string and c === string
      case _ =>
        ko
    }

    val error = "invalid lua"

    val string = "string"
  }
}
