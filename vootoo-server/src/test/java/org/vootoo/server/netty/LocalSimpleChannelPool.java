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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.connect.ChannelRefCounted;
import org.vootoo.client.netty.connect.SimpleChannelPool;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * @author chenlb on 2015-05-29 08:55.
 */
public class LocalSimpleChannelPool extends SimpleChannelPool {
  private static final Logger logger = LoggerFactory.getLogger(LocalSimpleChannelPool.class);
  protected SocketAddress serverAddress;

  protected LocalSimpleChannelPool(Bootstrap bootstrap, int poolSize, SocketAddress serverAddress, long connectTimeout) {
    super(bootstrap, poolSize, connectTimeout);
    this.serverAddress = serverAddress;
  }

  @Override
  protected SocketAddress getSocketAddress() {
    return serverAddress;
  }

  @Override
  protected ChannelRefCounted connectChannel(SocketAddress serverAddress, long connectTimeout) throws IOException {
    logger.info("try_connect {} ...", serverAddress.toString());
    return super.connectChannel(serverAddress, connectTimeout);
  }
}
