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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.connect.SimpleConnectionPool;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.client.netty.util.ByteStringer;
import org.vootoo.client.netty.util.ProtobufUtil;
import org.vootoo.common.MemoryOutputStream;
import org.vootoo.common.VootooException;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

/**
 * @author chenlb on 2015-05-28 10:12.
 */
public class NettySolrClientTest {

  private static final Logger logger = LoggerFactory.getLogger(NettySolrClientTest.class);

  static final String PORT = System.getProperty("port", "test_port");
  static final LocalAddress addr = new LocalAddress(PORT);

  static EventLoopGroup serverGroup = new LocalEventLoopGroup();
  static EventLoopGroup clientGroup = new NioEventLoopGroup(); // NIO event loops are also OK
  static Bootstrap client;
  static Channel serverChannel;

  protected static class MockResolver implements JavaBinCodec.ObjectResolver {
    @Override
    public Object resolve(Object o, JavaBinCodec codec) throws IOException {
      return o;
    }
  }

  protected static void writeResponse(NamedList<Object> values, OutputStream out) throws IOException {
    JavaBinCodec codec = new JavaBinCodec(new MockResolver());
    codec.marshal(values, out);
  }

  protected static class RequestParams {
    public static final String  ErrorCode = "_ErrorCode_";
    public static final String  ErrorMsg = "_ErrorMsg_";
    public static final String  ErrorMeta = "_ErrorMeta_";
    public static final String  ExceptionClass = "_ExceptionClass_";
    public static final String  WriteResponseId = "_WriteResponseId_";
  }

  protected static class MockSolrServerHandler extends SimpleChannelInboundHandler<SolrProtocol.SolrRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SolrProtocol.SolrRequest solrRequest) throws Exception {
      ProtobufRequestGetter requestGetter = new ProtobufRequestGetter(solrRequest);
      SolrParams solrParams = requestGetter.getSolrParams();

      SolrProtocol.SolrResponse.Builder protocolResponseBuilder = SolrProtocol.SolrResponse.newBuilder();
      protocolResponseBuilder.setRid(solrRequest.getRid());

      Integer errorCode = solrParams.getInt(RequestParams.ErrorCode);
      if(errorCode != null) {

        SolrProtocol.ExceptionBody.Builder exceptionBody = SolrProtocol.ExceptionBody.newBuilder();
        exceptionBody.setCode(errorCode);

        String[] msgs = solrParams.getParams(RequestParams.ErrorMsg);
        if(msgs != null) {
          for(String msg : msgs) {
            exceptionBody.addMessage(msg);
          }
        }

        String[] metas = solrParams.getParams(RequestParams.ErrorMeta);
        if(metas != null) {
          for(String meta : metas) {
            String[] ms = meta.split(":");
            if(ms != null && ms.length > 1) {
              exceptionBody.addMetadata(SolrProtocol.KeyValue.newBuilder().setKey(ms[0]).setValue(ms[1]));
            } else {
              exceptionBody.addMetadata(SolrProtocol.KeyValue.newBuilder().setKey(meta).setValue("notvalue"));
            }
          }
        }

        errorCode = VootooException.VootooErrorCode.getErrorCode(errorCode).code;

        if(errorCode == 500 || errorCode < 100) {
          logger.info("add exception trace test");
          exceptionBody.setTrace(SolrException.toStr(new RuntimeException("mock trace info")));
        }

        protocolResponseBuilder.addExceptionBody(exceptionBody);
      }

      String[] exceptionClass = solrParams.getParams(RequestParams.ExceptionClass);
      if(exceptionClass != null && exceptionClass.length > 0) {
        for(String excClass : exceptionClass) {
          SolrProtocol.ExceptionBody.Builder exceptionBody = SolrProtocol.ExceptionBody.newBuilder();
          Throwable throwable = null;
          try {
            Class clazz = Class.forName(excClass);
            Constructor<Throwable> constructor = clazz.getConstructor(String.class);
            throwable = constructor.newInstance("mock test ExceptionClass for junit");
            ProtobufUtil.getErrorInfo(throwable, exceptionBody);
          } catch (Throwable t) {
            ProtobufUtil.getErrorInfo(t, exceptionBody);
          }

          protocolResponseBuilder.addExceptionBody(exceptionBody);
        }
      }

      String responseId = solrParams.get(RequestParams.WriteResponseId);
      if(responseId != null) {
        //write query response
        SolrDocument sd = new SolrDocument();
        sd.addField("id", responseId);
        SolrDocumentList sdl = new SolrDocumentList();
        sdl.add(sd);

        sdl.setNumFound(1);
        sdl.setMaxScore(1.0f);
        sdl.setStart(0);

        NamedList<Object> values = new SimpleOrderedMap<>();
        NamedList<Object> responseHeader = new SimpleOrderedMap<>();
        values.add("responseHeader", responseHeader);

        values.add("response", sdl);

        responseHeader.add("status",0);
        responseHeader.add("QTime",10);

        MemoryOutputStream responeOutput = new MemoryOutputStream();
        writeResponse(values, responeOutput);

        SolrProtocol.ResponseBody.Builder responseBodyBuilder = SolrProtocol.ResponseBody.newBuilder();
        responseBodyBuilder.setBody(ByteStringer.wrap(responeOutput.getBuffer(), 0, responeOutput.getCount()));
        responseBodyBuilder.setContentType(BinaryResponseParser.BINARY_CONTENT_TYPE);

        protocolResponseBuilder.setResponseBody(responseBodyBuilder);
      }

      ctx.writeAndFlush(protocolResponseBuilder.build());
    }
  }

  protected static class MockSolrServerChannelInitializer extends ChannelInitializer<Channel> {

    @Override
    protected void initChannel(Channel ch) throws Exception {
      ChannelPipeline pipeline = ch.pipeline();

      pipeline.addLast("frame-decoder", new LengthFieldBasedFrameDecoder(50 * 1024 * 1024, 0, 4, 0, 4));
      pipeline.addLast("frame-encoder", new LengthFieldPrepender(4));

      pipeline.addLast("pb-decoder", new ProtobufDecoder(SolrProtocol.SolrRequest.getDefaultInstance()));
      pipeline.addLast("pb-encoder", new ProtobufEncoder());

      // add mock channel handler
      pipeline.addLast(new MockSolrServerHandler());
    }
  }

  @BeforeClass
  public static void start_local_netty() throws InterruptedException {
    ServerBootstrap sb = new ServerBootstrap();
    sb.group(serverGroup)
        .channel(LocalServerChannel.class)
        .handler(new ChannelInitializer<LocalServerChannel>() {
          @Override
          public void initChannel(LocalServerChannel ch) throws Exception {
            ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
          }
        })
        .childHandler(new MockSolrServerChannelInitializer());

    //client
    client = new Bootstrap();
    client.group(clientGroup)
        .channel(LocalChannel.class)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000);

    // Start the server.
    ChannelFuture serverFuture = sb.bind(addr).sync();

    serverChannel = serverFuture.channel();
  }

  @AfterClass
  public static void close_group() {
    if(serverChannel != null) {
      logger.info("server channel closing ...");
      serverChannel.close();
      logger.info("server channel closed");
    }
    if(serverGroup != null) {
      logger.info("server group closing ...");
      serverGroup.shutdownGracefully();
      logger.info("server group closed");
    }
    if(clientGroup != null) {
      logger.info("client group closing ...");
      clientGroup.shutdownGracefully();
      logger.info("client group closed");
    }
  }

  protected NettySolrClient createNettysolrClient() {
    //LocalSimpleChannelPool channelPool = new LocalSimpleChannelPool(client, 1, addr, 3000);
    SimpleConnectionPool connectionPool = new SimpleConnectionPool(client, addr);
    NettySolrClient solrClient = new NettySolrClient(connectionPool);
    return solrClient;
  }

  protected String createDocId() {
    return UUID.randomUUID().toString();
  }

  protected void assertIdResult(QueryResponse queryResponse, String idValue) {
    SolrDocumentList results = queryResponse.getResults();
    Assert.assertEquals(results.getNumFound(), 1);
    Assert.assertEquals(idValue, results.get(0).getFieldValue("id"));
  }

  protected SolrClient solrClient = null;

  @Before
  public void create_netty_solr_client() {
    solrClient = createNettysolrClient();
  }

  @After
  public void shutdown_netty_solr_client() {
    if(solrClient != null) {
      try {
        solrClient.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void test_netty_client_query() {
    String id = createDocId();
    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.set(RequestParams.WriteResponseId, id);

    try {
      QueryResponse query = solrClient.query(solrQuery);
      //System.out.println(query);
      assertIdResult(query, id);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  protected void assertVootooException(Throwable e, int code, SolrQuery solrQuery, boolean hasTrace) {
    Assert.assertTrue(e instanceof VootooException);
    Assert.assertEquals(((VootooException)e).code(), code);
    Assert.assertEquals(e.getMessage(), solrQuery.get(RequestParams.ErrorMsg));
    Assert.assertEquals(hasTrace, ((VootooException)e).getRemoteTrace() != null);
  }

  @Test
  public void test_netty_client_solr_exception() {
    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.add(RequestParams.ErrorCode, "503");
    solrQuery.add(RequestParams.ErrorMsg, "test 503 solr exception");
    solrQuery.add(RequestParams.ErrorMeta, "myKey:otherV");

    try {
      solrClient.query(solrQuery);

      Assert.fail("has Exception, but not throw");
    } catch (Exception e) {
      assertVootooException(e, 503, solrQuery, false);
    }

    solrQuery = new SolrQuery("*:*");
    solrQuery.add(RequestParams.ErrorCode, "429");
    solrQuery.add(RequestParams.ErrorMsg, "test 429 VootooException too many request");

    try {
      solrClient.query(solrQuery);
      Assert.fail("has Exception, but not throw");
    } catch (Exception e) {
      assertVootooException(e, 429, solrQuery, false);
    }

    //unknow
    int unknownCode = 1000000002;
    solrQuery = new SolrQuery("*:*");
    solrQuery.add(RequestParams.ErrorCode, String.valueOf(unknownCode));
    solrQuery.add(RequestParams.ErrorMsg, "test VootooException code="+unknownCode);

    try {
      solrClient.query(solrQuery);
      Assert.fail("has Exception, but not throw");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SolrServerException);
      Throwable throwable = ((SolrServerException) e).getCause();
      assertVootooException(throwable, VootooException.VootooErrorCode.UNKNOWN.code, solrQuery, true);

      VootooException ve = (VootooException) throwable;
      Assert.assertEquals(ve.getUnknownCode(), unknownCode);
      //System.out.println(ve.getRemoteServer());
      //System.out.println(ve.getRemoteTrace());
    }
  }

  @Test
  public void test_in_query_other_exception() {
    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.add(RequestParams.ExceptionClass, RejectedExecutionException.class.getName());
    //solrQuery.add(RequestParams.ExceptionClass, SolrException.class.getName());

    try {
      solrClient.query(solrQuery);
      Assert.fail("has Exception, but not throw");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof VootooException);

      VootooException ve = (VootooException) e;
      Assert.assertEquals(ve.code(), SolrException.ErrorCode.SERVER_ERROR.code);
      Assert.assertEquals(ve.getMessage(), "mock test ExceptionClass for junit");
      Assert.assertTrue(ve.getRemoteTrace() != null);
    }
  }
}