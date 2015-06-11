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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.NettyClient;
import org.vootoo.client.netty.NettySolrClient;
import org.vootoo.client.netty.SolrClientChannelInitializer;
import org.vootoo.client.netty.SolrClientHandler;
import org.vootoo.server.ExecutorConfig;
import org.vootoo.server.RequestExecutor;
import org.vootoo.server.RequestProcesserTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author chenlb on 2015-05-28 17:23.
 */
public class SolrServerHandlerTest extends RequestProcesserTest {

  private static final Logger logger = LoggerFactory.getLogger(SolrServerHandlerTest.class);

  static final String PORT = System.getProperty("port", "test_port");
  static final LocalAddress addr = new LocalAddress(PORT);

  static RequestExecutor queryExecutor;
  static RequestExecutor updateExecutor;
  static EventLoopGroup serverGroup = new LocalEventLoopGroup();
  static EventLoopGroup clientGroup = new NioEventLoopGroup(); // NIO event loops are also OK
  static Bootstrap client;
  static Channel serverChannel;

  @BeforeClass
  public static void start_local_netty() throws InterruptedException {
    queryExecutor = new RequestExecutor(ExecutorConfig.createDefault().setName("query-executor").setThreadNum(5));
    updateExecutor = new RequestExecutor(ExecutorConfig.createDefault().setName("update-executor").setThreadNum(2));
    // Note that we can use any event loop to ensure certain local channels
    // are handled by the same event loop thread which drives a certain socket channel
    // to reduce the communication latency between socket channels and local channels.
    ServerBootstrap sb = new ServerBootstrap();
    sb.group(serverGroup)
        .channel(LocalServerChannel.class)
        .handler(new ChannelInitializer<LocalServerChannel>() {
          @Override
          public void initChannel(LocalServerChannel ch) throws Exception {
            ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
          }
        })
        .childHandler(new SolrServerChannelInitializer(h.getCoreContainer(), new ChannelHandlerConfigs(),
            queryExecutor,
            updateExecutor
        ));

    client = new Bootstrap();
    client.group(clientGroup)
        .channel(LocalChannel.class)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
        .handler(new SolrClientChannelInitializer());

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

    if(queryExecutor != null) {
      logger.info("queryExecutor closing ...");
      queryExecutor.shutdownAndAwaitTermination(3, TimeUnit.SECONDS);
      logger.info("queryExecutor closed");
    }
    if(updateExecutor != null) {
      logger.info("updateExecutor closing ...");
      updateExecutor.shutdownAndAwaitTermination(3, TimeUnit.SECONDS);
      logger.info("updateExecutor closed");
    }
  }

  NettySolrClient solrClient;

  @Before
  public void before_test() {
    solrClient = createNettysolrClient();
  }

  protected NettySolrClient createNettysolrClient() {
    NettyClient nettyClient = new NettyClient(client);
    LocalSimpleChannelPool channelPool = new LocalSimpleChannelPool(client, 1, addr, 3000);

    NettySolrClient solrClient = new NettySolrClient("localhost", 8001, nettyClient, channelPool);
    return solrClient;
  }

  protected SolrQuery createIdQuery(String id) {
    SolrQuery query = new SolrQuery("id:\""+id+"\"");
    query.set("_timeout_", 1000);
    query.set("indent", "on");
    return query;
  }

  @Test
  public void test_query() throws InterruptedException, IOException, SolrServerException {
    String id = addTestDoc();

    QueryResponse queryResponse = solrClient.query("collection1", createIdQuery(id));

    assertIdResult(queryResponse, id);
  }

  @Test
  public void test_update_add() throws IOException, SolrServerException {
    String id = createDocId();

    SolrInputDocument sid = new SolrInputDocument();
    sid.addField("id", id);

    UpdateResponse addResp = solrClient.add("collection1", sid);

    System.out.println(addResp);

    assertU(commit());

    assertQueryId(id);
  }

}