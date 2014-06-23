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
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.vootoo.client.netty.NettyUtil;
import org.vootoo.client.netty.SolrRequest;
import org.vootoo.client.netty.protocol.SolrProtocol;

import com.google.protobuf.ByteString;

/**
 */
public class SolrRequestDecoder extends MessageToMessageDecoder<SolrProtocol.SolrRequest> {

  @Override
  protected void decode(ChannelHandlerContext ctx, SolrProtocol.SolrRequest msg, List<Object> out) throws Exception {
    SolrRequest solrRequest = new SolrRequest();
    solrRequest.setRid(msg.getRid());
    solrRequest.setCoreName(msg.getCoreName());
    solrRequest.setPath(msg.getPath());

    String params = msg.getParams().toStringUtf8();

    solrRequest.setSolrParams(NettyUtil.fromString(params));

    String contentType = solrRequest.getSolrParams().get("contentType");

    Collection<ContentStream> streams = new ArrayList<ContentStream>(msg.getStreamsCount());

    for(ByteString stream : msg.getStreamsList()) {
      ContentStreamBase content = NettyUtil.readFrom(stream);
      content.setContentType(contentType);
      streams.add(content);
    }

    out.add(solrRequest);
  }
}
