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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.Channel;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.connect.ChannelPool;
import org.vootoo.client.netty.connect.ChannelRefCounted;
import org.vootoo.client.netty.connect.SimpleChannelPool;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.client.netty.util.ProtobufUtil;

/**
 */
public class NettySolrClient extends SolrClient {
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(NettySolrClient.class);

  private static final String DEFAULT_PATH = "/select";

  private NettyClient nettyClient;

  private final String host;
  private final int port;

  private ChannelPool channelPool;

  protected ModifiableSolrParams invariantParams;
  protected ResponseParser parser = new BinaryResponseParser();

  protected NettyBinaryRequestWriter requestWriter = new NettyBinaryRequestWriter();

  public NettySolrClient(String host, int port) {
    this(host, port, NettyClient.DEFAULT);
  }

  public NettySolrClient(String host, int port, NettyClient nettyClient) {
    this(host, port, nettyClient, new SimpleChannelPool(nettyClient.getBootstrap(), 1, new InetSocketAddress(host, port), 2000));
  }
  public NettySolrClient(String host, int port, NettyClient nettyClient, ChannelPool channelPool) {
    this.host = host;
    this.port = port;
    this.nettyClient = nettyClient;
    this.channelPool = channelPool;
  }

  protected ResponseParser createRequest(SolrRequest request, ProtobufRequestSetter saveRequestSetter) throws SolrServerException, IOException {

    SolrParams params = request.getParams();
    Collection<ContentStream> streams = requestWriter.getContentStreams(request);//throw IOException
    String path = requestWriter.getPath(request);
    if (path == null || !path.startsWith("/")) {
      path = DEFAULT_PATH;
    }

    ResponseParser parser = request.getResponseParser();
    if (parser == null) {
      parser = this.parser;
    }

    // The parser 'wt=' and 'version=' params are used instead of the original
    // params
    ModifiableSolrParams wparams = new ModifiableSolrParams(params);
    if (parser != null) {
      wparams.set(CommonParams.WT, parser.getWriterType());
      wparams.set(CommonParams.VERSION, parser.getVersion());
    }
    if (invariantParams != null) {
      wparams.add(invariantParams);
    }

    saveRequestSetter.setPath(path);
    saveRequestSetter.setSolrParams(wparams);

    if (SolrRequest.METHOD.GET == request.getMethod()) {
      if (streams != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
      }
      return parser;
    }

    if (SolrRequest.METHOD.POST == request.getMethod() || SolrRequest.METHOD.PUT == request.getMethod()) {
      if (streams != null) {
        saveRequestSetter.setContentStreams(streams);
      }

      return parser;
    }

    throw new SolrServerException("Unsupported method: " + request.getMethod());
  }

  @Override
  public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
    int timeout = request.getParams().getInt("_timeout_", 3000);

    ChannelRefCounted channelRef = null;

    try {
      final ProtobufRequestSetter protobufRequestSetter = new ProtobufRequestSetter();
      if(collection != null) {
        protobufRequestSetter.setCollection(collection);
      }

      ResponseParser parser = createRequest(request, protobufRequestSetter);

      final SolrParams solrParams = protobufRequestSetter.getSolrParams();

      channelRef = channelPool.getChannel(3);

      final Channel channel = channelRef.get();

      ResponseCallback responseCallBack = new ResponseCallback();
      long rid = 0;
      do {
        rid = NettyClient.createRid();
        ResponseCallback oldCallBack = NettyClient.responseCallbacks.putIfAbsent(rid, responseCallBack);
        if(oldCallBack == null) {
          break;
        }
      } while(true);

      protobufRequestSetter.setRid(rid);

      //send netty request
      try {
        channel.writeAndFlush(protobufRequestSetter.buildProtocolRequest()).sync();
      } catch (InterruptedException e) {
        throw new IOException("channel.writeAndFlush throw InterruptedException, rid="+protobufRequestSetter.getRid()+" request=["+solrParams+"]");
      }

      SolrProtocol.SolrResponse protocolResponse = responseCallBack.awaitResult(timeout);

      if(protocolResponse == null) {
        throw new TimeoutException("rid="+rid+" request=["+request.getParams()+"] is timeout="+timeout+", request=["+solrParams+"]");
      }

      InputStream responseInputStream = ProtobufUtil.getSolrResponseInputStream(protocolResponse);

      if(responseInputStream == null) {
        // not response body
        return new NamedList<>();
      }

      String charset = ProtobufUtil.getResponseBodyCharset(protocolResponse);

      return parser.processResponse(responseInputStream, charset);
    } catch (TimeoutException e) {
      throw new SolrServerException(e.getMessage(), e);
    } finally {
      if(channelRef != null) {
        channelRef.decref();
      }
    }
  }


  @Override
  public void shutdown () {
    channelPool.decref();
  }

  public ModifiableSolrParams getInvariantParams() {
    return invariantParams;
  }

  public ResponseParser getParser() {
    return parser;
  }

  public void setParser(ResponseParser parser) {
    this.parser = parser;
  }
}
