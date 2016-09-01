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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.slf4s.Logging
import sys.process._

case class SSH(
  machine: Machine,
  remoteCommand: Option[String] = None,
  ttyAllocated: Boolean = false,
  retryEnabled: Boolean = false,
  includeAWSCredentials: Boolean = false
)(implicit conf: ClusterConf) extends Logging {
  def withRemoteCommand(cmd: String) = this.copy(remoteCommand = Some(cmd))
  def withTTY = this.copy(ttyAllocated = true)
  def withRetry = this.copy(retryEnabled = true)
  def withAWSCredentials = this.copy(includeAWSCredentials = true)

  def getCommandSeq(maskAWS: Boolean) = Seq(
    "ssh",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "StrictHostKeyChecking=no"
  )
    .++(conf.pem.fold(Seq.empty[String])(pem => Seq("-i", pem)))
    .++(if (ttyAllocated) Some("-tt") else None)
    .:+(conf.user + "@" + machine.address)
    .++(remoteCommand.map { remoteCommand =>
      if (includeAWSCredentials) {
        //get aws credentials in formal way
        val credentials = new DefaultAWSCredentialsProviderChain().getCredentials
        Seq(
          "AWS_ACCESS_KEY_ID='" + (if (maskAWS) "*" else credentials.getAWSAccessKeyId) + "'",
          "AWS_SECRET_ACCESS_KEY='" + (if (maskAWS) "*" else credentials.getAWSSecretKey) + "'",
          remoteCommand
        ).mkString(" ")
      } else remoteCommand
    })

  def run(): Int = {
    val op = (attempt: Int) => {
      log.info(s"[${machine.name}] [attempt:${attempt}] ${getCommandSeq(true)}")
      val exitValue = getCommandSeq(false).!
      if (exitValue != 0) {
        sys.error(s"[${machine.name}] Error when running '${getCommandSeq(true)}'. exitValue = ${exitValue}.")
      } else exitValue
    }
    if (retryEnabled) Retry(op) else op(1)
  }
}
