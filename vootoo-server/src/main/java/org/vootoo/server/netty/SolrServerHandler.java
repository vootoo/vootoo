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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.FastByteOutputStream;
import org.vootoo.client.netty.SolrRequest;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.server.ErrorCodeSetter;
import org.vootoo.server.SolrDispatcher;

import com.google.protobuf.ByteString;

/**
 */
public class SolrServerHandler extends SimpleChannelInboundHandler<SolrRequest> {

  private static final Logger logger = LoggerFactory.getLogger(SolrServerHandler.class);

  private final CoreContainer coreContainer;

  public SolrServerHandler (CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  private static class SolrProtocolErrorCodeSetter implements ErrorCodeSetter {

    final SolrProtocol.SolrResponse.Builder solrResponseBuilder;

    private SolrProtocolErrorCodeSetter(
        SolrProtocol.SolrResponse.Builder solrResponseBuilder) {
      this.solrResponseBuilder = solrResponseBuilder;
    }

    @Override
    public void setError(int errorCode, String errorStr) {
      solrResponseBuilder.setErrorCode(errorCode);
      solrResponseBuilder.setErrorStr(errorStr);
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, SolrRequest solrRequest) throws Exception {
    SolrDispatcher dispatcher = new SolrDispatcher(solrRequest, coreContainer);

    dispatcher.run();

    SolrProtocol.SolrResponse.Builder solrResponseBuilder = SolrProtocol.SolrResponse.newBuilder();
    solrResponseBuilder.setRid(solrRequest.getRid());

    FastByteOutputStream outputStream = new FastByteOutputStream();

    if(dispatcher.getThrowable() != null) {
      dispatcher.sendError(outputStream, new SolrProtocolErrorCodeSetter(solrResponseBuilder), dispatcher.getThrowable());
    } else {
      dispatcher.writeResponse(outputStream);
      solrResponseBuilder.setResponseFormat(dispatcher.getResponseFormat());
      solrResponseBuilder.setResponse(ByteString.copyFrom(outputStream.buffer(), 0, outputStream.bufferSize()));
    }

    // write solr response to channel
    ctx.writeAndFlush(solrResponseBuilder);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    logger.info("channelReadComplete, channel={}", ctx.channel());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Unexpected exception from downstream.", cause);
    ctx.close();
  }


}
