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

package org.apache.linkis.manager.common.protocol.engine;

import org.apache.linkis.governance.common.entity.ExecutionNodeStatus;
import org.apache.linkis.protocol.message.RequestProtocol;

/** engineConn to LM */
public class OnceJobStatusCallback implements RequestProtocol {

  private final Long jobId;
  private final ExecutionNodeStatus status;
  private final String initErrorMsg;

  public OnceJobStatusCallback(Long jobId, ExecutionNodeStatus status, String initErrorMsg) {
    this.jobId = jobId;
    this.status = status;
    this.initErrorMsg = initErrorMsg;
  }

  public Long getJobId() {
    return jobId;
  }

  public ExecutionNodeStatus getStatus() {
    return status;
  }

  public String getInitErrorMsg() {
    return initErrorMsg;
  }

  @Override
  public String toString() {
    return "OnceJobStatusCallback{"
        + "jobId="
        + jobId
        + ", status="
        + status
        + ", initErrorMsg='"
        + initErrorMsg
        + '\''
        + '}';
  }
}
