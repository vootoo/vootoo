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

import io.netty.channel.*;

import io.netty.util.concurrent.Promise;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.protocol.SolrProtocol;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class SolrClientHandler extends SimpleChannelInboundHandler<SolrProtocol.SolrResponse> {
  private static final Logger logger = LoggerFactory.getLogger(SolrClientHandler.class);

  protected static final String CLIENT_HANDLER_NAME = "vootoo_solr_handler";

  protected final ResponsePromiseContainer responsePromiseContainer;

  public SolrClientHandler(ResponsePromiseContainer responsePromiseContainer) {
    if(responsePromiseContainer == null) {
      throw new IllegalArgumentException("ResponsePromise maps can't be null!");
    }
    this.responsePromiseContainer = responsePromiseContainer;
  }

  public static SolrClientHandler getSolrClientHandler(Channel channel) {
    SolrClientHandler clientHandler = (SolrClientHandler) channel.pipeline().get(CLIENT_HANDLER_NAME);
    assert clientHandler != null;
    return clientHandler;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, SolrProtocol.SolrResponse solrResponse) throws Exception {
    ResponsePromise responsePromise = responsePromiseContainer.removeResponsePromise(solrResponse.getRid());
    if (responsePromise != null) {
      logger.debug("receive response={}", solrResponse);
      responsePromise.receiveResponse(solrResponse);
    } else {
      SocketAddress remoteAddress = ctx.channel().remoteAddress();
      logger.warn("miss rid='{}' at {} receive response", solrResponse.getRid(), remoteAddress);
      responsePromise.setFailure(new IllegalStateException("receive not registered response, rid="+solrResponse.getRid()+", server="+remoteAddress));
    }

  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Unexpected exception from downstream. close channel="+ctx.channel(), cause);
    ctx.close();
  }

  private Promise<SolrProtocol.SolrResponse> newPromise(Channel channel) {
    return channel.eventLoop().next().<SolrProtocol.SolrResponse>newPromise();
  }

  public ChannelFuture writeRequest(Channel channel, SolrProtocol.SolrRequest solrRequest, final ResponsePromise responsePromise) {
    responsePromise.setResponsePromise(newPromise(channel));
    ChannelFuture writeFuture = channel.writeAndFlush(solrRequest);
    return writeFuture;
  }

  public void removeResponsePromise(long rid) {
    responsePromiseContainer.removeResponsePromise(rid);
  }
}
