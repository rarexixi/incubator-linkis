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

package org.apache.linkis.manager.am.service;

import org.apache.linkis.governance.common.entity.job.JobRequest;
import org.apache.linkis.governance.common.protocol.job.JobReqUpdate;
import org.apache.linkis.governance.common.protocol.job.JobRespProtocol;
import org.apache.linkis.manager.am.conf.AMConfiguration;
import org.apache.linkis.protocol.message.RequestProtocol;
import org.apache.linkis.rpc.Sender;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** From Entrance */
public class JobHistoryService {

  public static final JobHistoryService INSTANCE = new JobHistoryService();
  private static final int MAX_DESC_LEN = AMConfiguration.ERROR_CODE_DESC_LEN;
  private Sender sender;

  private static final Logger logger = LoggerFactory.getLogger(JobHistoryService.class);

  private JobHistoryService() {
    sender = Sender.getSender(AMConfiguration.JOBHISTORY_SPRING_APPLICATION_NAME.getValue());
  }

  public void updateStatus(Long jobId, String status, String msg) {
    logger.info("Change job [{}] status to {}", jobId, status);
    JobRequest jobRequest = new JobRequest();
    jobRequest.setId(jobId);
    jobRequest.setStatus(status);
    jobRequest.setErrorDesc(msg);
    update(jobRequest);
  }

  public void update(JobRequest jobRequest) {
    if (null == jobRequest) {
      return;
    }
    if (null != jobRequest.getErrorDesc()) {
      // length of errorDesc must be less then 1000 / 3, because length of errorDesc in db is
      // 1000
      if (jobRequest.getErrorDesc().length() > MAX_DESC_LEN) {
        jobRequest.setErrorDesc(jobRequest.getErrorDesc().substring(0, MAX_DESC_LEN));
      }
    }
    jobRequest.setUpdatedTime(new Date());
    JobReqUpdate jobReqUpdate = new JobReqUpdate(jobRequest);
    sendToJobHistoryAndRetry(
        jobReqUpdate, "job:" + jobRequest.getReqId() + "status:" + jobRequest.getStatus());
  }

  private JobRespProtocol sendToJobHistoryAndRetry(RequestProtocol jobReq, String msg) {
    JobRespProtocol jobRespProtocol = null;
    int retryTimes = 0;
    boolean retry = true;
    while (retry && retryTimes < AMConfiguration.JOBINFO_UPDATE_RETRY_MAX_TIME.getHotValue()) {
      try {
        retryTimes++;
        jobRespProtocol = (JobRespProtocol) sender.ask(jobReq);
        if (jobRespProtocol.getStatus() == 2) {
          logger.warn(
              "Request jobHistory failed, joReq msg{}, retry times: {}, reason {}",
              msg,
              retryTimes,
              jobRespProtocol.getMsg());
        } else {
          retry = false;
        }
      } catch (Exception e) {
        logger.warn(
            "Request jobHistory failed, joReq msg{}, retry times: {}, reason {}",
            msg,
            retryTimes,
            e);
      }
      if (retry) {
        try {
          Thread.sleep(AMConfiguration.JOBINFO_UPDATE_RETRY_INTERVAL.getHotValue());
        } catch (Exception ex) {
          logger.warn(ex.getMessage());
        }
      }
    }
    if (jobRespProtocol != null) {
      int status = jobRespProtocol.getStatus();
      String message = jobRespProtocol.getMsg();
      if (status != 0) {
        logger.warn("Request jobHistory failed,because:{} (请求jobHistory失败)", message);
      }
    } else {
      logger.warn(
          "Request jobHistory failed, because:jobRespProtocol is null (请求jobHistory失败,因为jobRespProtocol为null)");
    }
    return jobRespProtocol;
  }
}
