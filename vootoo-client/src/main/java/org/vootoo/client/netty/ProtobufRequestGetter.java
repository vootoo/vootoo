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

package org.vootoo.client.netty;

import java.util.Collection;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.client.netty.util.ProtobufUtil;
import org.vootoo.RequestGetter;

/**
 * decode protobuf solr request for server
 */
public class ProtobufRequestGetter implements RequestGetter {

  private final SolrProtocol.SolrRequest protocolSolrRequest;

  /** receive SolrRequest time that begin completely netty receive and parse protobuf to SolrRequest*/
  private long receiveTime;
  private SolrParams solrParams;
  private Collection<ContentStream> contentStreams;

  public ProtobufRequestGetter(SolrProtocol.SolrRequest protocolSolrRequest) {
    this.protocolSolrRequest = protocolSolrRequest;
    receiveTime = System.currentTimeMillis();
  }

  public SolrParams getSolrParams() {
    if(solrParams == null) {
      solrParams = ProtobufUtil.toSolrParams(protocolSolrRequest);
    }
    return solrParams;
  }
  
  public Collection<ContentStream> getContentStreams() {
    if(contentStreams == null) {
      contentStreams = ProtobufUtil.toSolrContentStreams(protocolSolrRequest);
    }

    return contentStreams;
  }

  public String getCollection() {
    String col = protocolSolrRequest.getCollection();
    return col == null ? "" : col;
  }
  
  public String getPath() {
    return protocolSolrRequest.getPath();
  }

  public long getRid() {
    return protocolSolrRequest.getRid();
  }

  public long getReceiveTime() {
    return receiveTime;
  }

  /**
   * @param expectTimeout ms
   * @return
   */
  public boolean isTimeout(int expectTimeout) {
    return (System.currentTimeMillis() - receiveTime) > expectTimeout;
  }
}
