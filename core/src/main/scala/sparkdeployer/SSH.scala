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

import Helpers.retry
import org.slf4s.Logging
import sys.process._

case class SSH(
  address: String,
  remoteCommand: Option[String] = None,
  ttyAllocated: Boolean = false,
  retryEnabled: Boolean = false,
  runningMessage: Option[String] = None,
  errorMessage: Option[String] = None,
  includeAWSCredentials: Boolean = false
)(implicit clusterConf: ClusterConf) extends Logging {
  def withRemoteCommand(cmd: String) = this.copy(remoteCommand = Some(cmd))
  def withTTY = this.copy(ttyAllocated = true)
  def withRetry = this.copy(retryEnabled = true)
  def withRunningMessage(msg: String) = this.copy(runningMessage = Some(msg))
  def withErrorMessage(msg: String) = this.copy(errorMessage = Some(msg))
  def withAWSCredentials = this.copy(includeAWSCredentials = true)

  private def fullCommandSeq(maskAWS: Boolean) = Seq(
    "ssh",
    "-i", clusterConf.pem,
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "StrictHostKeyChecking=no"
  )
    .++(if (ttyAllocated) Some("-tt") else None)
    .:+(clusterConf.user + "@" + address)
    .++(remoteCommand.map { remoteCommand =>
      if (includeAWSCredentials) {
        Seq(
          "AWS_ACCESS_KEY_ID='" + (if (maskAWS) "*" else sys.env.get("AWS_ACCESS_KEY_ID").getOrElse("")) + "'",
          "AWS_SECRET_ACCESS_KEY='" + (if (maskAWS) "*" else sys.env.get("AWS_SECRET_ACCESS_KEY").getOrElse("")) + "'",
          remoteCommand
        ).mkString(" ")
      } else remoteCommand
    })

  def getCommand = fullCommandSeq(true).mkString(" ")

  def run(): Int = {
    val op = (attempts: Int) => {
      log.info("[SSH] " + runningMessage.getOrElse("ssh") + s" Attempts: $attempts. Command: " + fullCommandSeq(true).mkString(" "))
      val exitValue = fullCommandSeq(false).!
      if (exitValue != 0) {
        sys.error(s"[SSH] ${errorMessage.getOrElse("ssh error")}. exitValue = ${exitValue}.")
      } else exitValue
    }
    if (retryEnabled) retry(op) else op(1)
  }
}
