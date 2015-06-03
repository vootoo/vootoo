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
import org.vootoo.client.netty.ProtobufRequestGetter;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.server.RequestExecutor;
import org.vootoo.server.Vootoo;
import org.vootoo.server.RequestProcesser;

import java.util.concurrent.RejectedExecutionException;

/**
 */
public class SolrServerHandler extends SimpleChannelInboundHandler<SolrProtocol.SolrRequest> {

  private static final Logger logger = LoggerFactory.getLogger(SolrServerHandler.class);

  private final CoreContainer coreContainer;
  private final RequestExecutor queryExecutor;
  private final RequestExecutor updateExecutor;

  public SolrServerHandler(CoreContainer coreContainer, RequestExecutor queryExecutor, RequestExecutor updateExecutor) {
    this.coreContainer = coreContainer;
    this.queryExecutor = queryExecutor;
    this.updateExecutor = updateExecutor;
  }

  private class SolrRequestRunner implements Runnable {
    private ChannelHandlerContext ctx;
    private ProtobufRequestGetter solrRequest;

    public SolrRequestRunner(ChannelHandlerContext ctx, ProtobufRequestGetter solrRequest) {
      this.ctx = ctx;
      this.solrRequest = solrRequest;
    }

    @Override
    public void run() {
      ProtobufResponseSetter responeSetter = new ProtobufResponseSetter(solrRequest.getRid());
      try {
        handleRequest(responeSetter, solrRequest);
      } catch (Throwable t) {
        responeSetter.addError(t);
      } finally {
        // write solr protocol response to channel
        ctx.writeAndFlush(responeSetter.buildProtocolResponse());
      }
    }

    protected void handleRequest(ProtobufResponseSetter responeSetter, ProtobufRequestGetter solrRequest) {
      RequestProcesser requestHandler = new RequestProcesser(coreContainer, responeSetter);
      requestHandler.handleRequest(solrRequest);
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, SolrProtocol.SolrRequest solrRequest) throws Exception {
    SolrRequestRunner solrTask = new SolrRequestRunner(ctx, new ProtobufRequestGetter(solrRequest));

    try {
      // solr execute request in thread pool
      if(Vootoo.isUpdateRequest(solrRequest.getPath())) {
        if(logger.isDebugEnabled()) {
          logger.debug("execute update={} rid={} bs={}", new Object[] {solrRequest.getPath(), solrRequest.getRid(), solrRequest.getSerializedSize()});
        }
        updateExecutor.submitTask(solrTask, SolrProtocol.SolrResponse.class);
      } else {// query request
        queryExecutor.submitTask(solrTask, SolrProtocol.SolrResponse.class);
      }
    } catch (RejectedExecutionException e) {

    }
    //TODO throwable more detail info for different scene

  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    logger.debug("channelReadComplete, channel={}", ctx.channel());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Netty Channel Handler Exception channel=" + ctx.channel(), cause);
    ctx.close();
  }

}
