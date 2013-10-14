package redis
package algebra
package interpreter
package nonblocking

import org.specs2.ScalaCheck
import org.specs2.matcher.Parameters
import org.specs2.specification.FragmentsBuilder

trait ScalaCheckSpecification extends ScalaCheck with FragmentsBuilder {
  implicit val params = Parameters(verbose = true)
}
