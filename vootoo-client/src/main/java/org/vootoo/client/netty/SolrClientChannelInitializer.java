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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.vootoo.client.netty.protocol.SolrProtocol;

/**
 */
public class SolrClientChannelInitializer extends ChannelInitializer<Channel> {

  private static int MB = 1024 * 1024;

  protected final ResponsePromiseContainer responsePromiseContainer;

  public SolrClientChannelInitializer(ResponsePromiseContainer responsePromiseContainer) {
    if(responsePromiseContainer == null) {
      throw new IllegalArgumentException("ResponsePromise maps can't be null!");
    }
    this.responsePromiseContainer = responsePromiseContainer;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline.addLast("frame-decoder", new LengthFieldBasedFrameDecoder(200 * MB, 0, 4, 0, 4));
    pipeline.addLast("frame-encoder", new LengthFieldPrepender(4));

    pipeline.addLast("pb-decoder", new ProtobufDecoder(SolrProtocol.SolrResponse.getDefaultInstance()));
    pipeline.addLast("pb-encoder", new ProtobufEncoder());

    pipeline.addLast(SolrClientHandler.CLIENT_HANDLER_NAME, new SolrClientHandler(responsePromiseContainer));
  }

  public ResponsePromiseContainer getResponsePromiseContainer() {
    return responsePromiseContainer;
  }
}
