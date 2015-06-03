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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.connect.ChannelRefCounted;
import org.vootoo.client.netty.connect.NettyConnectLessException;

/**
 */
public class NettyClient {
  private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

  protected static final AtomicLong ridSeed = new AtomicLong(0);
  protected static final ConcurrentMap<Long,ResponseCallback> responseCallbacks = new ConcurrentHashMap<Long,ResponseCallback>();

  public static NettyClient DEFAULT = new NettyClient();

  private EventLoopGroup loopGroup;
  private Bootstrap bootstrap;

  public NettyClient() {
    loopGroup = new NioEventLoopGroup(10);
    bootstrap = new Bootstrap();
    bootstrap.group(loopGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000);
    bootstrap.handler(new SolrClientChannelInitializer());
  }

  public NettyClient(Bootstrap bootstrap) {
    this.bootstrap = bootstrap;
  }

  public void shutdown() {
    if(loopGroup != null) {
      loopGroup.shutdownGracefully();
    }
  }

  public Bootstrap getBootstrap() {
    return bootstrap;
  }

  public static long createRid() {
    return ridSeed.incrementAndGet();
  }

  public static void put(long rid, ResponseCallback callback) {
    responseCallbacks.put(rid, callback);
  }

  public static ResponseCallback remove(long rid) {
    return responseCallbacks.remove(rid);
  }

  protected ChannelRefCounted connectChannel(InetSocketAddress serverAddress, long connectTimeout) throws IOException {
    return connect(bootstrap, serverAddress, connectTimeout);
  }

  /**
   * @param bootstrap
   * @param serverAddress
   * @param connectTimeout
   * @return ChannelRefCounted 已经 incref 一次，完了后，还要 decref。
   * @throws IOException
   */
  public static ChannelRefCounted connect(Bootstrap bootstrap, final SocketAddress serverAddress, long connectTimeout) throws NettyConnectLessException {

    ChannelFuture future = null;
    try {
      future = bootstrap.connect(serverAddress).sync();
    } catch (InterruptedException e) {
      throw new NettyConnectLessException("connect fail by InterruptedException, tcp=" + serverAddress);
    }

    // use bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000);
    // so await without timeout
    future.awaitUninterruptibly();

    assert future.isDone();

    if(future.isCancelled()) {
      // Connection attempt cancelled by user
      throw new NettyConnectLessException("Connection attempt cancelled by user tcp=" + serverAddress);
    } else if(!future.isSuccess()) {
      throw new NettyConnectLessException(future.cause().getMessage()+", tcp="+serverAddress, future.cause());
    } else {
      // Connection established successfully
      if(logger != null) {
        logger.info("connect remote server={} success", serverAddress);
      }

      //加到 ref
      ChannelRefCounted channelRef = new ChannelRefCounted(future.channel());
      channelRef.incref();
      return channelRef;
    }

  }
}
