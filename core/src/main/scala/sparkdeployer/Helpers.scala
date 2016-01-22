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

import org.slf4s.Logging
import scala.util.{Failure, Success, Try}

object Helpers extends Logging {
  @annotation.tailrec
  private def retry[T](op: Int => T, attempts: Int): T = {
    Try { op(attempts) } match {
      case Success(x) => x
      case Failure(e) if attempts > 1 =>
        log.warn("Exception catched, will retry.", e)
        Thread.sleep(15000)
        retry(op, attempts - 1)
      case Failure(e) => throw e
    }
  }
  def retry[T](op: Int => T)(implicit clusterConf: ClusterConf): T = retry(op, clusterConf.retryAttempts)
}
