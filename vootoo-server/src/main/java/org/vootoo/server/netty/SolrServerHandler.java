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
import org.vootoo.common.VootooException;
import org.vootoo.server.RequestExecutor;
import org.vootoo.server.Vootoo;

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

  private class NettySolrRequestRunner extends SolrRequestRunner {

    private final ChannelHandlerContext ctx;

    public NettySolrRequestRunner(ChannelHandlerContext ctx, ProtobufRequestGetter solrRequest) {
      super(coreContainer, solrRequest);
      this.ctx = ctx;
    }

    @Override
    protected String getSocketAddress() {
      return ctx.channel().remoteAddress().toString();
    }

    @Override
    protected void writeProtocolResponse(SolrProtocol.SolrResponse protocolResponse) {
      ctx.writeAndFlush(protocolResponse);
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, SolrProtocol.SolrRequest solrRequest) throws Exception {
    SolrRequestRunner solrTask = new NettySolrRequestRunner(ctx, new ProtobufRequestGetter(solrRequest));

    try {
      // solr execute request in thread pool
      if(Vootoo.isUpdateRequest(solrRequest.getPath())) {
        solrTask.setIsUpdate(true);
        updateExecutor.submitTask(solrTask, SolrProtocol.SolrResponse.class);
      } else {// query request
        solrTask.setIsUpdate(false);
        queryExecutor.submitTask(solrTask, SolrProtocol.SolrResponse.class);
      }
    } catch (RejectedExecutionException e) {
      solrTask.logRejected();
      //server is too busy
      ProtobufResponseSetter responeSetter = new ProtobufResponseSetter(solrRequest.getRid());
      responeSetter.addError(VootooException.VootooErrorCode.TOO_MANY_REQUESTS.code, "RequestExecutor thread pool is full, server is too busy!");
      ctx.writeAndFlush(responeSetter.buildProtocolResponse());
    }
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
