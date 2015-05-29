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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.client.netty.util.ByteStringer;
import org.vootoo.common.MemoryOutputStream;
import org.vootoo.server.ResponseSetter;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

/**
 * @author chenlb on 2015-05-25 17:28.
 */
public class ProtobufResponseSetter implements ResponseSetter<SolrProtocol.SolrResponse> {

  private static final Logger logger = LoggerFactory.getLogger(ProtobufResponseSetter.class);

  private SolrProtocol.SolrResponse.Builder protocolResponseBuilder = SolrProtocol.SolrResponse.newBuilder();
  private SolrProtocol.ResponseBody.Builder responseBodyBuilder;

  private SolrQueryResponse solrQueryResponse;
  private MemoryOutputStream responeOutput;

  public ProtobufResponseSetter(long rid) {
    protocolResponseBuilder.setRid(rid);
  }

  private void checkResponseBodyBuilder() {
    if(responseBodyBuilder == null) {
      responseBodyBuilder = SolrProtocol.ResponseBody.newBuilder();
    }
  }

  @Override
  public void setContentType(String contentType) {
    checkResponseBodyBuilder();
    responseBodyBuilder.setContentType(contentType);
  }

  @Override
  public void setStatus(int status) {
    checkResponseBodyBuilder();
    responseBodyBuilder.setStatus(status);
  }

  @Override
  public OutputStream getOutputStream() {
    if(responeOutput == null) {
      responeOutput = new MemoryOutputStream();
    }
    return responeOutput;
  }

  @Override
  public void sendError(int code, Throwable ex) {
    protocolResponseBuilder.setErrorCode(code);
    protocolResponseBuilder.setErrorMsg(SolrException.toStr(ex));
    logger.error(ex.getMessage(), ex);
  }

  @Override
  public void writeQueryResponseComplete(SolrQueryResponse solrQueryResponse) {
    this.solrQueryResponse = solrQueryResponse;
    if(responeOutput != null) {
      checkResponseBodyBuilder();
      responseBodyBuilder.setBody(ByteStringer.wrap(responeOutput.getBuffer(), 0, responeOutput.getCount()));
      protocolResponseBuilder.setResponseBody(responseBodyBuilder);
    } else {
      logger.warn("SolrQueryResponse write completed, but not found in OutputStream, responeOutput is null.");
    }
  }

  @Override
  public SolrQueryResponse getSolrQueryResponse() {
    return solrQueryResponse;
  }

  @Override
  public SolrProtocol.SolrResponse buildProtocolResponse() {
    return protocolResponseBuilder.build();
  }
}
