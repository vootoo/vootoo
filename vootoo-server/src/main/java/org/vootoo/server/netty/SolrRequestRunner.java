/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.vootoo.server.netty;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.vootoo.client.netty.ProtobufRequestGetter;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.common.VootooException;
import org.vootoo.server.RequestProcesser;
import org.vootoo.server.Vootoo;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author chenlb on 2015-06-04 09:15.
 */
public abstract class SolrRequestRunner implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(SolrRequestRunner.class);
  private static final Logger queryLogger = LoggerFactory.getLogger(Vootoo.requestLogName("Query"));
  private static final Logger updateLogger = LoggerFactory.getLogger(Vootoo.requestLogName("Update"));
  private static final Logger rejectedLogger = LoggerFactory.getLogger(Vootoo.requestLogName("Rejected"));

  private CoreContainer coreContainer;
  private ProtobufRequestGetter solrRequest;
  private boolean isUpdate = false;

  private int queryDefaultTimeout = 2000;

  public SolrRequestRunner(CoreContainer coreContainer, ProtobufRequestGetter solrRequest) {
    this.coreContainer = coreContainer;
    this.solrRequest = solrRequest;
  }

  protected void putMDC() {
    MDC.put(Vootoo.MDC_NAME_RID, String.valueOf(solrRequest.getRid()));
    MDC.put(Vootoo.MDC_NAME_REQUEST_SIZE, String.valueOf(solrRequest.requestSize()));
    MDC.put(Vootoo.MDC_NAME_REQUEST_PATH, solrRequest.getPath());
  }

  protected void putMDCWithExecuteTime(long executeTime) {
    MDC.put(Vootoo.MDC_NAME_EXECUTE_TIME, String.valueOf(executeTime));
  }

  protected void putMDCWithWaitTime(long waitTime) {
    MDC.put(Vootoo.MDC_NAME_WAIT_TIME, String.valueOf(waitTime));
  }

  protected void putMDCWithRemoteAddress(String remoteAddress) {
    MDC.put(Vootoo.MDC_NAME_REMOTE_ADDRESS, remoteAddress);
  }

  protected void clearMDC() {
    MDC.remove(Vootoo.MDC_NAME_RID);
    MDC.remove(Vootoo.MDC_NAME_REQUEST_SIZE);
    MDC.remove(Vootoo.MDC_NAME_REQUEST_PATH);
    MDC.remove(Vootoo.MDC_NAME_EXECUTE_TIME);
    MDC.remove(Vootoo.MDC_NAME_WAIT_TIME);
    MDC.remove(Vootoo.MDC_NAME_REMOTE_ADDRESS);
  }

  protected abstract String getSocketAddress();

  @Override
  public void run() {
    clearMDC();
    putMDC();
    putMDCWithRemoteAddress(getSocketAddress());

    ProtobufResponseSetter responseSetter = new ProtobufResponseSetter(solrRequest.getRid());
    try {
      handleRequest(responseSetter, solrRequest);
    } catch (Throwable t) {
      responseSetter.addError(t);
    } finally {
      // write solr protocol response to channel
      writeProtocolResponse(responseSetter.buildProtocolResponse());
    }
  }

  protected abstract void writeProtocolResponse(SolrProtocol.SolrResponse protocolResponse);

  protected void handleRequest(ProtobufResponseSetter responeSetter, ProtobufRequestGetter solrRequest) {
    long waitTime = solrRequest.useTime();
    //mdc log wait in queue time
    putMDCWithWaitTime(waitTime);

    Logger myLogger = isUpdate ? updateLogger : queryLogger;

    SolrParams solrParams = solrRequest.getSolrParams();
    int timeout = solrParams.getInt(CommonParams.TIME_ALLOWED, queryDefaultTimeout);

    if (waitTime > timeout) {//is timeout
      myLogger.warn("tip={} params={{}}", "Timeout_In_Queue", solrParams);
      throw new VootooException(VootooException.VootooErrorCode.TIMEOUT, "timeout stay in RequestQueue");
    }

    if(myLogger.isDebugEnabled()) {
      myLogger.debug("tip={} params={{}}", "Start_Execute", solrRequest.getSolrParams());
    }

    RequestProcesser requestHandler = new RequestProcesser(coreContainer, responeSetter);

    long st = System.currentTimeMillis();
    requestHandler.handleRequest(solrRequest);
    long ut = System.currentTimeMillis() - st;

    //mdc log execute time
    putMDCWithExecuteTime(ut);

    //log
    if(responeSetter.getSolrQueryResponse() != null && responeSetter.getSolrQueryResponse().getToLog().size() > 0) {
      NamedList<Object> myToLog = responeSetter.getSolrQueryResponse().getToLog();
      String plog = toLogString(myToLog);

      myLogger.info("{}", plog);
    } else {
      myLogger.info("params={{}}", solrParams);
    }
  }

  protected static final Set<String> toLogNames;

  static {
    Set<String> logNames = new HashSet<>();
    logNames.add("params");
    logNames.add("hits");
    logNames.add("status");
    logNames.add("QTime");

    toLogNames = Collections.unmodifiableSet(logNames);
  }

  protected String toLogString(NamedList<Object> toLog) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<toLog.size(); i++) {
      String name = toLog.getName(i);
      if(!toLogNames.contains(name)) {
        continue;
      }
      Object val = toLog.getVal(i);
      if (name != null) {
        sb.append(name).append('=');
      }
      sb.append(val).append(' ');
    }
    return sb.toString();
  }

  protected void logRejected() {
    clearMDC();
    putMDC();
    putMDCWithRemoteAddress(getSocketAddress());
    rejectedLogger.warn("tip={} params={{}}", "Request_Rejected", solrRequest.getSolrParams());
  }

  public void setIsUpdate(boolean isUpdate) {
    this.isUpdate = isUpdate;
  }
}
