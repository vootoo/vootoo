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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.server.ExecutorConfig;
import org.vootoo.server.RequestExecutor;

/**
 * solr netty server
 */
public class SolrNettyServer {
  private static final Logger logger = LoggerFactory.getLogger(SolrNettyServer.class);

  private final int port;

  private final CoreContainer coreContainer;

  private RequestExecutor queryExecutor;
  private RequestExecutor updateExecutor;

  protected ExecutorConfig queryConfig = ExecutorConfig.createDefault().setName("query-executor");
  protected ExecutorConfig updateConfig = ExecutorConfig.createDefault().setName("update-executor").setThreadNum(40);

  private int bossWorkerNum = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
  private int ioWorkerNum = Runtime.getRuntime().availableProcessors() * 2;
  protected ChannelHandlerConfigs handlerConfigs = new ChannelHandlerConfigs();

  protected volatile Channel serverChannel;

  protected Thread asyncNettyRunner;

  public SolrNettyServer(CoreContainer cores, int port) {
    this.coreContainer = cores;
    this.port = port;
  }

  public void startServer(boolean sync) throws Exception {
    if(sync) {
      startNetty();
    } else {
      asyncNettyRunner = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            startNetty();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }, "async-netty-runner");

      asyncNettyRunner.start();
    }
  }

  protected void startNetty() throws Exception {
    logger.info("netty server starting port={} ...", port);
    queryExecutor = new RequestExecutor(queryConfig);
    updateExecutor = new RequestExecutor(updateConfig);

    EventLoopGroup bossGroup = new NioEventLoopGroup(bossWorkerNum);
    EventLoopGroup workerGroup = new NioEventLoopGroup(ioWorkerNum);
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new SolrServerChannelInitializer(coreContainer, handlerConfigs, queryExecutor, updateExecutor));

      // Start the server.
      ChannelFuture f = b.bind(port).sync();

      logger.info("netty server started port={}", port);

      // Wait until the server socket is closed.
      serverChannel = f.channel();
      serverChannel.closeFuture().sync();
    } finally {
      // Shut down all event loops to terminate all threads.
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();

      // Wait until all threads are terminated.
      bossGroup.terminationFuture().sync();
      workerGroup.terminationFuture().sync();
    }
  }

  public void stopServer() throws Exception {
    if(serverChannel != null) {
      logger.info("server channel closing ...");
      serverChannel.close();
      logger.info("server channel is closed");
    }
  }

  public int getBossWorkerNum() {
    return bossWorkerNum;
  }

  public void setBossWorkerNum(int bossWorkerNum) {
    this.bossWorkerNum = bossWorkerNum;
  }

  public int getIoWorkerNum() {
    return ioWorkerNum;
  }

  public void setIoWorkerNum(int ioWorkerNum) {
    this.ioWorkerNum = ioWorkerNum;
  }

  public ExecutorConfig getQueryConfig() {
    return queryConfig;
  }

  public void setQueryConfig(ExecutorConfig queryConfig) {
    this.queryConfig = queryConfig;
  }

  public ExecutorConfig getUpdateConfig() {
    return updateConfig;
  }

  public void setUpdateConfig(ExecutorConfig updateConfig) {
    this.updateConfig = updateConfig;
  }
}
