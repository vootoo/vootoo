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
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.PackageInfo;
import org.vootoo.client.netty.connect.*;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.client.netty.util.ProtobufUtil;
import org.vootoo.common.VootooException;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 */
public class NettySolrClient extends SolrClient {
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(NettySolrClient.class);
  private static final Logger requestLogger = LoggerFactory.getLogger(PackageInfo.packageName()+".Request");

  private static final String DEFAULT_PATH = "/select";

  // default timeout
  private int defaultTimeout = 2000;

  private final String serverUrl;

  private ConnectionPool connectionPool;

  protected ModifiableSolrParams invariantParams;
  protected ResponseParser parser = new BinaryResponseParser();
  protected RequestWriter requestWriter = new BinaryRequestWriter();

  public NettySolrClient(String host, int port) {
    this(new SimpleConnectionPool(NettyClient.DEFAULT.getBootstrap(), new InetSocketAddress(host, port)));
  }
  public NettySolrClient(ConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
    serverUrl = "netty://"+connectionPool.channelHost()+":"+connectionPool.channelPort();
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

    Integer timeout = wparams.getInt(CommonParams.TIME_ALLOWED);
    if(timeout == null) {
      //mandatory use TIME_ALLOWED
      timeout = defaultTimeout;
      wparams.set(CommonParams.TIME_ALLOWED, timeout);
    }

    saveRequestSetter.setTimeout(timeout);
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
    final ProtobufRequestSetter protobufRequestSetter = new ProtobufRequestSetter();
    if(collection != null) {
      protobufRequestSetter.setCollection(collection);
    }

    ResponseParser parser = createRequest(request, protobufRequestSetter);

    final SolrParams solrParams = protobufRequestSetter.getSolrParams();

    //CommonParams.TIME_ALLOWED + 100ms
    int timeout = protobufRequestSetter.getTimeout() + 100;

    Long rid = null;
    try {
      ResponseCallback responseCallBack = new ResponseCallback();
      do {
        rid = NettyClient.createRid();
        ResponseCallback oldCallBack = NettyClient.putIfAbsent(rid, responseCallBack);
        if(oldCallBack == null) {
          break;
        }
      } while(true);

      protobufRequestSetter.setRid(rid);

      Channel channel = null;
      //send netty request
      try {
        channel = connectionPool.acquireConnect();
        // sync ?
        channel.writeAndFlush(protobufRequestSetter.buildProtocolRequest()).sync();
      } catch (InterruptedException e) {
        throw new IOException("channel.writeAndFlush throw InterruptedException, rid="+rid+", ["+serverUrl()+"] request=["+solrParams+"]");
      } finally {
        if(channel != null) {
          connectionPool.releaseConnect(channel);
        }
      }

      SolrProtocol.SolrResponse protocolResponse = null;

      try {
        protocolResponse = responseCallBack.awaitResult(timeout);
      } catch (InterruptedException e) {
        logger.warn("await response interrupted, blank result instead of. rid="+protobufRequestSetter.getRid());
        return new NamedList<>();
      }

      if(protocolResponse == null) {
        VootooException vootooException = new VootooException(VootooException.VootooErrorCode.TIMEOUT,
            "rid=" + rid + ", [" + serverUrl() + "] is timeout=" + timeout + ", request=[" + solrParams + "]");
        vootooException.setRemoteServer(serverUrl());
        throw vootooException;
      }

      if(logger.isDebugEnabled()) {
        logger.debug("rid={}, response size={}", rid, protocolResponse.getSerializedSize());
      }

      // protocolResponse error
      if(protocolResponse.getExceptionBodyCount() > 0) {
        List<VootooException> solrExcs = new ArrayList<>(protocolResponse.getExceptionBodyCount());
        VootooException firstE = null;
        int eNum = 0;
        //has exception
        for(SolrProtocol.ExceptionBody e : protocolResponse.getExceptionBodyList()) {
          VootooException se = ProtobufUtil.toVootooException(e);
          se.setRemoteServer(serverUrl());
          if(se.code() != VootooException.ErrorCode.UNKNOWN.code) {
            //use first SolrException without ErrorCode.UNKNOWN
            if(firstE == null) {
              firstE = se;
            }
          }
          solrExcs.add(se);

          eNum++;
          if(logger.isDebugEnabled()) {
            logger.debug("[WARN] rid={}, [{}] response exception ({}/{}), code={}, msg={}, meta={}, trace={}", new Object[] {
                rid, se.getRemoteServer(), eNum, protocolResponse.getExceptionBodyCount(),
                se.code(), se.getMessage(), se.getMetadata(), se.getRemoteTrace()
            });
          } else if(logger.isInfoEnabled()) {
            logger.info("[WARN] rid={}, [{}] response exception ({}/{}), code={}, msg={}, meta={}", new Object[] {
                rid, se.getRemoteServer(), eNum, protocolResponse.getExceptionBodyCount(),
                se.code(), se.getMessage(), se.getMetadata()
            });
          }

        }//for exc body

        if(firstE != null) {
          logger.warn("rid={}, [{}] response exception, ErrorCode={}, msg={}, meta={}", new Object[] {
              rid, firstE.getRemoteServer(), firstE.code(), firstE.getMessage(), firstE.getMetadata()
          });
          //meta contain server url
          throw firstE;
        } else {
          //all Exception is ErrorCode.UNKNOWN
          //convert SolrServerException
          throw new SolrServerException("rid="+rid+", ["+serverUrl()+"] has unknow error", solrExcs.get(0));
        }
      }//has exception

      InputStream responseInputStream = ProtobufUtil.getSolrResponseInputStream(protocolResponse);

      if(responseInputStream == null) {
        // not response body
        logger.warn("rid={}, [{}] not response body, params={}", rid, serverUrl(), solrParams);
        return new NamedList<>();
      }

      String charset = ProtobufUtil.getResponseBodyCharset(protocolResponse);

      return parser.processResponse(responseInputStream, charset);
    } finally {
      if(rid != null) {
        NettyClient.remove(rid);
      }
    }
  }

  public String serverUrl() {
    return serverUrl;
  }

  @Override
  public void shutdown () {
    connectionPool.close();
  }

  public ModifiableSolrParams getInvariantParams() {
    return invariantParams;
  }

  public ResponseParser getParser() {
    return parser;
  }

  /**
   * Note: This setter method is <b>not thread-safe</b>.
   *
   * @param responseParser
   *          Default Response Parser chosen to parse the response if the parser
   *          were not specified as part of the request.
   * @see org.apache.solr.client.solrj.SolrRequest#getResponseParser()
   */
  public void setParser(ResponseParser responseParser) {
    this.parser = responseParser;
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }
}
