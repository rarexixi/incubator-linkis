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

package org.apache.linkis.orchestrator.ecm

import org.apache.linkis.common.ServiceInstance
import org.apache.linkis.common.exception.LinkisRetryException
import org.apache.linkis.common.utils.{ByteTimeUtils, Logging, Utils}
import org.apache.linkis.governance.common.conf.GovernanceCommonConf
import org.apache.linkis.governance.common.entity.job.JobRequest
import org.apache.linkis.manager.common.entity.node.EngineNode
import org.apache.linkis.manager.common.protocol.engine.{EngineCreateRequest, EngineStopRequest}
import org.apache.linkis.manager.label.entity.engine.EngineConnMode
import org.apache.linkis.manager.label.utils.LabelUtil
import org.apache.linkis.orchestrator.computation.conf.ComputationOrchestratorConf
import org.apache.linkis.orchestrator.ecm.conf.ECMPluginConf
import org.apache.linkis.orchestrator.ecm.exception.ECMPluginErrorException
import org.apache.linkis.protocol.constants.TaskConstant
import org.apache.linkis.protocol.utils.TaskUtils
import org.apache.linkis.rpc.Sender

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import java.net.{SocketException, SocketTimeoutException}
import java.util

import scala.collection.JavaConverters._

object OnceEngineConnManager extends Logging {

  private val applyTime: Long = ECMPluginConf.ECM_MARK_APPLY_TIME.getValue.toLong
  private val attemptNumber: Int = ECMPluginConf.ECM_MARK_ATTEMPTS.getValue
  private val createService = ComputationOrchestratorConf.DEFAULT_CREATE_SERVICE.getValue

  def submitJob(job: JobRequest): EngineNode = {

    val engineCreateRequest = getEngineCreateRequest(job)
    var count = attemptNumber
    var retryException: LinkisRetryException = null
    while (count >= 1) {
      count = count - 1
      val start = System.currentTimeMillis()
      try {
        val (engineNode, _) = getEngineNodeAskManager(job, engineCreateRequest)
        if (null != engineNode) {
          logger.info(
            s"${job.getId} Success to ask engineCreateRequest $engineCreateRequest, engineNode: $engineNode"
          )
          return engineNode
        }
      } catch {
        case t: LinkisRetryException =>
          val taken = ByteTimeUtils.msDurationToString(System.currentTimeMillis - start)
          logger.warn(
            s"${job.getId} Failed to engineCreateRequest time taken ($taken), ${t.getMessage}"
          )
          retryException = t
        case t: Throwable =>
          val taken = ByteTimeUtils.msDurationToString(System.currentTimeMillis - start)
          logger.warn(s"${job.getId} Failed to engineCreateRequest time taken ($taken)")
          throw t
      }
    }
    if (retryException != null) {
      throw retryException
    } else {
      throw new ECMPluginErrorException(
        ECMPluginConf.ECM_ERROR_CODE,
        s"${job.getId} Failed to ask engineCreateRequest $engineCreateRequest by retry ${attemptNumber - count}  "
      )
    }
  }

  def killJob(job: JobRequest): Unit = {
    val engineConnMode = LabelUtil.getEngineConnMode(job.getLabels)
    if (!EngineConnMode.isOnceMode(engineConnMode)) {
      return
    }
    val engineconnMap = job.getMetrics.getOrDefault("engineconnMap", null)
    engineconnMap match {
      case ecMap: util.Map[String, Object] => {
        if (!ecMap.isEmpty) {
          ecMap.values.toArray()(0) match {
            case map: util.Map[String, String] =>
              val appName = map.getOrDefault(TaskConstant.EXECUTEAPPLICATIONNAME, "")
              val engineInstance = map.getOrDefault(TaskConstant.ENGINE_INSTANCE, "")
              val ticketId = map.getOrDefault(TaskConstant.TICKET_ID, "")
              if (
                  StringUtils.isNotBlank(appName) && StringUtils.isNotBlank(
                    engineInstance
                  ) && StringUtils.isNotBlank(ticketId)
              ) {
                val serviceInstance: ServiceInstance = ServiceInstance(appName, engineInstance)
                val engineStopRequest = new EngineStopRequest(serviceInstance, job.getSubmitUser)
                Utils.tryCatch(getManagerSender.ask(engineStopRequest)) { t: Throwable =>
                  val baseMsg = s"job ${job.getId} failed to ask LinkisManager to stopEngine "
                  ExceptionUtils.getRootCause(t) match {
                    case socketTimeoutException: SocketTimeoutException =>
                      val msg = baseMsg + ExceptionUtils.getMessage(socketTimeoutException)
                      throw new LinkisRetryException(
                        ECMPluginConf.ECM_ENGNE_CREATION_ERROR_CODE,
                        msg
                      )
                    case socketException: SocketException =>
                      val msg = baseMsg + ExceptionUtils.getMessage(socketException)
                      throw new LinkisRetryException(
                        ECMPluginConf.ECM_ENGNE_CREATION_ERROR_CODE,
                        msg
                      )
                    case _ =>
                      throw t
                  }
                }
              }
            case _ =>
          }
        }
      }
      case _ =>
    }
  }

  private def getEngineNodeAskManager(
      job: JobRequest,
      engineCreateRequest: EngineCreateRequest
  ): (EngineNode, Boolean) = {
    val response = Utils.tryCatch(getManagerSender.ask(engineCreateRequest)) { t: Throwable =>
      val baseMsg = s"job ${job.getId} failed to ask LinkisManager "
      ExceptionUtils.getRootCause(t) match {
        case socketTimeoutException: SocketTimeoutException =>
          val msg = baseMsg + ExceptionUtils.getMessage(socketTimeoutException)
          throw new LinkisRetryException(ECMPluginConf.ECM_ENGNE_CREATION_ERROR_CODE, msg)
        case socketException: SocketException =>
          val msg = baseMsg + ExceptionUtils.getMessage(socketException)
          throw new LinkisRetryException(ECMPluginConf.ECM_ENGNE_CREATION_ERROR_CODE, msg)
        case _ =>
          throw t
      }
    }
    response match {
      case engineNode: EngineNode =>
        logger.debug(s"Succeed to create engineNode $engineNode jobId: ${job.getId}")
        (engineNode, true)
      case _ =>
        logger.info(
          s"${job.getId} Failed to ask engineCreateRequest ${engineCreateRequest}, response is not engineNode"
        )
        (null, false)
    }
  }

  private def getEngineCreateRequest(job: JobRequest): EngineCreateRequest = {

    val engineCreateRequest = new EngineCreateRequest
    val labelMap = new util.HashMap[String, Object]()
    job.getLabels.asScala.foreach(label => {
      labelMap.put(label.getLabelKey, label.getStringValue)
    })
    engineCreateRequest.setLabels(labelMap)
    engineCreateRequest.setTimeout(applyTime)
    engineCreateRequest.setUser(job.getSubmitUser)

    val properties = new util.HashMap[String, String]
    val startupMap = TaskUtils.getStartupMap(job.getParams)
    if (startupMap != null) {
      startupMap.asScala.foreach { case (k, v) =>
        if (v != null && StringUtils.isNotEmpty(v.toString)) properties.put(k, v.toString)
      }
    }
    engineCreateRequest.setProperties(properties)
    engineCreateRequest.setCreateService(createService)
    engineCreateRequest
  }

  private def getManagerSender: Sender = {
    Sender.getSender(GovernanceCommonConf.MANAGER_SERVICE_NAME.getValue)
  }

}
