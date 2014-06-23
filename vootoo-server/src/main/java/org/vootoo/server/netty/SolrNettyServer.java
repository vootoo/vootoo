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
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.solr.core.CoreContainer;
import org.vootoo.core.CoreContainerUtil;

/**
 * solr netty server
 */
public class SolrNettyServer {

  private final int port;

  private final CoreContainer coreContainer;

  public SolrNettyServer (CoreContainer cores, int port) {
    this.coreContainer = cores;
    this.port = port;
  }

  public void run() throws Exception {
    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new SolrServerChannelInitializer(coreContainer));

      // Start the server.
      ChannelFuture f = b.bind(port).sync();

      // Wait until the server socket is closed.
      f.channel().closeFuture().sync();
    } finally {
      // Shut down all event loops to terminate all threads.
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();

      // Wait until all threads are terminated.
      bossGroup.terminationFuture().sync();
      workerGroup.terminationFuture().sync();
    }
  }

  public static void main (String[] args) throws Exception {
    if(args.length < 2) {
      System.out.println(SolrNettyServer.class.getSimpleName()+" <solr-home> <nettp-port>");
      System.exit(1);
    }
    String solrHome = args[0];
    int port = Integer.parseInt(args[1]);
    CoreContainer coreContainer = CoreContainerUtil.createCoreContainer(solrHome);
    SolrNettyServer sns = new SolrNettyServer(coreContainer, port);
    sns.run();
  }
}
