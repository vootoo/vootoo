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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.connect.ChannelPool;
import org.vootoo.client.netty.connect.ChannelRefCounted;
import org.vootoo.client.netty.connect.SimpleChannelPool;
import org.vootoo.client.netty.protocol.SolrProtocol;

import com.google.protobuf.ByteString;

/**
 */
public class NettySolrServer extends SolrServer {
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(NettySolrServer.class);

  private NettyClient nettyClient = NettyClient.DEFAULT;

  private final String host;
  private final int port;

  private ChannelPool channelPool;

  protected ResponseParser responseParser = new BinaryResponseParser();
  protected NettyBinaryRequestWriter requestWriter = new NettyBinaryRequestWriter();

  public NettySolrServer(String host, int port) {
    this.host = host;
    this.port = port;

    channelPool = new SimpleChannelPool(nettyClient.getBootstrap(), 1, new InetSocketAddress(host, port), 2000);
  }

  @Override
  public NamedList<Object> request (SolrRequest request) throws SolrServerException, IOException {
    String path = requestWriter.getPath(request);
    assert path != null;

    ModifiableSolrParams params = null;
    if(request.getParams() == null) {
      params = new ModifiableSolrParams();
    } else {
      params = new ModifiableSolrParams(request.getParams());
    }

    params.set(CommonParams.WT, responseParser.getWriterType());
    params.set(CommonParams.VERSION, responseParser.getVersion());

    Collection<ContentStream> streams = requestWriter.getContentStreams(request);

    org.vootoo.client.netty.SolrRequest solrRequest = new org.vootoo.client.netty.SolrRequest();
    solrRequest.setRid(NettyClient.createRid());
    //solrRequest.setCoreName(request.getPath());
    solrRequest.setPath(request.getPath());
    solrRequest.setSolrParams(params);
    solrRequest.setContentStreams(streams);

    AwaitResponseCallback callback = new AwaitResponseCallback();
    NettyClient.put(solrRequest.getRid(), callback);

    ChannelRefCounted channelRefCounted = channelPool.getChannel(2);

    writeRequest(solrRequest, channelRefCounted);

    SolrProtocol.SolrResponse solrResponse = callback.awaitResponse(-1);
    if(solrResponse == null) {
      return new NamedList<Object>();
    }

    ByteString bytes = solrResponse.getResponse();
    if(bytes == null) {
      return new NamedList<Object>();
    }

    //ResponseFormat format = ResponseFormat.valueOf(solrResponse.getResponseFormat());

    return responseParser.processResponse(bytes.newInput(), "utf-8");
  }

  private void writeRequest(org.vootoo.client.netty.SolrRequest solrRequest, ChannelRefCounted channelRefCounted) {
    Channel channel = channelRefCounted.get();
    channel.write(solrRequest);
    channel.flush();
  }

  @Override
  public void shutdown () {
    channelPool.decref();
  }
}
