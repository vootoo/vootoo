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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.NettyClient;

/**
 */
public class SimpleChannelPool implements ChannelPool {
  private static final Logger logger = LoggerFactory.getLogger(SimpleChannelPool.class);

  private final Bootstrap bootstrap;
  private int poolSize;

  protected AtomicInteger idxCursor = new AtomicInteger();
  protected ArrayList<ChannelRefCounted> pool;

  protected InetSocketAddress serverAddress;
  protected long connectTimeout;

  public SimpleChannelPool(Bootstrap bootstrap, int poolSize, InetSocketAddress serverAddress, long connectTimeout) {
    this.bootstrap = bootstrap;
    this.poolSize = poolSize;
    pool = new ArrayList<ChannelRefCounted>(poolSize);

    this.serverAddress = serverAddress;
    this.connectTimeout = connectTimeout;
  }

  @Override
  public String channelHost() {
    return serverAddress.getAddress().getHostAddress();
  }

  @Override
  public int channelPort() {
    return serverAddress.getPort();
  }

  protected ChannelRefCounted connectChannel(InetSocketAddress serverAddress, long connectTimeout) throws IOException {
    return NettyClient.connect(bootstrap, serverAddress, connectTimeout);
  }

  @Override
  public ChannelRefCounted getChannel(int maxTry) throws IOException {
    if(pool.size() < poolSize) {
      ChannelRefCounted newChannel = connectChannel(serverAddress, connectTimeout);
      pool.add(newChannel);
      return newChannel;
    }
    int idx = idxCursor.getAndIncrement()%pool.size();
    ChannelRefCounted channel = pool.get(idx);
    if(!channel.isAvailable()) {
      synchronized (channel) {
        if(!channel.isAvailable()) {
          ChannelRefCounted newChannel = connectChannel(serverAddress, connectTimeout);
          pool.set(idx, newChannel);
          //逐出后要 dec ref
          channel.decref();
          //新的换上
          channel = newChannel;
        }
      }
    }
    channel.incref();
    return channel;
  }

  @Override
  public void decref() {
    for(ChannelRefCounted ch : pool) {
      ch.decref();
    }
  }

  @Override
  public void close() {
    for(ChannelRefCounted ch : pool) {
      ch.close();
    }
  }

  @Override
  public long getConnectTimeout() {
    return connectTimeout;
  }

  @Override
  public void setConnectTimeout(long connectTimeout) {
    this.connectTimeout = connectTimeout;
  }
}
