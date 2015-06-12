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

package org.vootoo.client.netty.connect;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.local.LocalAddress;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.SolrClientChannelPoolHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author chenlb on 2015-06-12 09:55.
 */
public class SimpleConnectionPool extends io.netty.channel.pool.SimpleChannelPool implements ConnectionPool {
  private static final Logger logger = LoggerFactory.getLogger(SimpleConnectionPool.class);

  protected static final int DEFAULT_CONNECT_TIMEOUT = 2000;

  protected final SocketAddress socketAddress;
  protected final String host;
  protected final int port;

  protected int connectTimeout; //default 2s

  /**
   * auto use {@link SolrClientChannelPoolHandler} for solr netty channel
   */
  public SimpleConnectionPool(Bootstrap bootstrap, SocketAddress remoteAddress) {
    this(bootstrap, new SolrClientChannelPoolHandler(remoteAddress), remoteAddress, DEFAULT_CONNECT_TIMEOUT);
  }

  public SimpleConnectionPool(Bootstrap bootstrap, SolrClientChannelPoolHandler handler, SocketAddress socketAddress, int connectTimeout) {
    super(bootstrap, handler);
    this.connectTimeout = connectTimeout;
    this.socketAddress = socketAddress;
    if(socketAddress instanceof InetSocketAddress) {
      InetSocketAddress inetSocketAddress = (InetSocketAddress)socketAddress;
      host = inetSocketAddress.getAddress().getHostAddress();
      port = inetSocketAddress.getPort();
    } else if(socketAddress instanceof LocalAddress) {
      LocalAddress localAddress = (LocalAddress) socketAddress;
      int myPort = -1;
      try {
        myPort = Integer.parseInt(localAddress.id());
      } catch (NumberFormatException e){}

      host = "local";
      port = myPort;
    } else {
      throw new IllegalArgumentException("SocketAddress must be '"+InetSocketAddress.class.getName()+"' or '"+LocalAddress.class.getName()+"' (sub) class");
    }
  }

  @Override
  protected ChannelFuture connectChannel(Bootstrap bs) {
    bs.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
    return bs.connect(socketAddress);
  }

  public Channel acquireConnect() throws NettyConnectLessException {
    Future<Channel> future = acquire();

    // see https://netty.io/4.0/api/io/netty/channel/ChannelFuture.html
    // use bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
    // so await without timeout
    future.awaitUninterruptibly();

    assert future.isDone();

    if(future.isCancelled()) {
      // Connection attempt cancelled by user
      throw new NettyConnectLessException("connection cancelled tcp=" + socketAddress);
    } else if(!future.isSuccess()) {
      throw new NettyConnectLessException("connect tcp="+socketAddress+" fail within "+connectTimeout+"ms time!", future.cause());
    } else {
      // Connection established successfully
      Channel channel = future.getNow();

      if(logger.isDebugEnabled()) {
        logger.debug("acquire connect success channel={}", channel);
      }

      assert channel != null;

      if(channel == null) {
        throw new NettyConnectLessException("connect tcp="+socketAddress+" fail within "+connectTimeout+"ms time, future.getNow return null!");
      }

      return channel;
    }
  }

  @Override
  public void releaseConnect(Channel channel) {
    Future<Void> future = release(channel);

    //TODO  wait future done ?
  }

  @Override
  public String channelHost() {
    return host;
  }

  @Override
  public int channelPort() {
    return port;
  }

  public void setConnectTimeout(int connectTimeout) {
    if(connectTimeout < 10) {
      throw new IllegalArgumentException("connectTimeout can't < 10ms");
    }
    this.connectTimeout = connectTimeout;
  }

}
