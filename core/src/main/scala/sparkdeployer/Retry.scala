/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sparkdeployer

import scala.util.{Failure, Success, Try}
import org.slf4s.Logging

object Retry extends Logging {
  @annotation.tailrec
  def apply[T](op: Int => T, attempt: Int, maxAttempts: Int): T = {
    Try { op(attempt) } match {
      case Success(x) => x
      case Failure(e) if attempt < maxAttempts =>
        Thread.sleep(15000)
        apply(op, attempt + 1, maxAttempts)
      case Failure(e) => throw e
    }
  }
  def apply[T](op: Int => T)(implicit clusterConf: ClusterConf): T = apply(op, 1, clusterConf.retryAttempts)
}
