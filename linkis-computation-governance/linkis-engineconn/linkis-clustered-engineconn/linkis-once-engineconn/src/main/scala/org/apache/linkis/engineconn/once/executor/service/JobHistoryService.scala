/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.linkis.engineconn.once.executor.service

import org.apache.linkis.common.utils.{Logging, Utils}
import org.apache.linkis.engineconn.executor.conf.EngineConnExecutorConfiguration
import org.apache.linkis.governance.common.entity.job.JobRequest
import org.apache.linkis.governance.common.protocol.job.{JobReqUpdate, JobRespProtocol}
import org.apache.linkis.protocol.message.RequestProtocol
import org.apache.linkis.rpc.Sender

import java.util.Date

/**
 * copy from QueryPersistenceEngine in Entrance
 */
object JobHistoryService extends Logging {

  private val MAX_DESC_LEN: Int = EngineConnExecutorConfiguration.ERROR_CODE_DESC_LEN

  private val sender: Sender =
    Sender.getSender(EngineConnExecutorConfiguration.JOBHISTORY_SPRING_APPLICATION_NAME.getValue)

  def updateStatus(jobId: Long, status: String): Unit = {
    logger.info("Change job [{}] status to {}", jobId, status)
    if (jobId != null) {}
    val jobRequest: JobRequest = new JobRequest
    jobRequest.setId(jobId)
    jobRequest.setStatus(status)
    update(jobRequest)
  }

  def update(jobRequest: JobRequest): Unit = {
    if (null == jobRequest) { return }
    if (null != jobRequest.getErrorDesc) {
      // length of errorDesc must be less then 1000 / 3
      // because length of errorDesc in db is 1000
      if (jobRequest.getErrorDesc.length > MAX_DESC_LEN) {
        jobRequest.setErrorDesc(jobRequest.getErrorDesc.substring(0, MAX_DESC_LEN))
      }
    }
    jobRequest.setUpdatedTime(new Date)
    val jobReqUpdate: JobReqUpdate = new JobReqUpdate(jobRequest)
    sendToJobHistoryAndRetry(
      jobReqUpdate,
      "job:" + jobRequest.getReqId + "status:" + jobRequest.getStatus
    )
  }

  private def sendToJobHistoryAndRetry(jobReq: RequestProtocol, msg: String): JobRespProtocol = {
    var jobRespProtocol: JobRespProtocol = null
    var retryTimes: Int = 0
    var retry: Boolean = true
    val retryMaxTime = EngineConnExecutorConfiguration.JOBINFO_UPDATE_RETRY_MAX_TIME.getHotValue
    val retryInterval = EngineConnExecutorConfiguration.JOBINFO_UPDATE_RETRY_INTERVAL.getHotValue
    while (retry && retryTimes < retryMaxTime) {
      try {
        retryTimes += 1
        jobRespProtocol = sender.ask(jobReq).asInstanceOf[JobRespProtocol]
        if (jobRespProtocol.getStatus == 2) {
          logger.error(
            s"Request jobHistory failed, joReq msg $msg, retry times: $retryTimes, reason ${jobRespProtocol.getMsg}"
          )
        } else {
          retry = false
        }
      } catch {
        case e: Exception =>
          logger.error(s"Request jobHistory failed, joReq msg $msg, retry times: $retryTimes", e)
      }
      if (retry) {
        Utils.sleepQuietly(retryInterval)
      }
    }
    if (jobRespProtocol != null) {
      val status: Int = jobRespProtocol.getStatus
      val message: String = jobRespProtocol.getMsg
      if (status != 0) {
        logger.warn("Request jobHistory failed,because:{} (请求jobHistory失败)", message)
      }
    } else {
      logger.warn(
        "Request jobHistory failed, because:jobRespProtocol is null (请求jobHistory失败,因为jobRespProtocol为null)"
      )
    }
    jobRespProtocol
  }

}
