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

import io.netty.channel.Channel;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ChannelRefCounted extends RefCount<Channel> {

  private static final Logger logger = LoggerFactory.getLogger(ChannelRefCounted.class);

  private static AtomicLong ID = new AtomicLong();

  private long myId = ID.incrementAndGet();

  public ChannelRefCounted(Channel resource) {
    super(resource);
  }

  public long getMyId() {
    return myId;
  }

  @Override
  protected void close() {
    logger.info("close channel[{}]={} ...", myId, resource.remoteAddress());
    resource.close();
  }

  public SocketAddress remoteAddress() {
    if(resource == null) {
      return null;
    } else {
      return resource.remoteAddress();
    }
  }

  public boolean isAvailable() {
    return resource != null && resource.isOpen() && resource.isActive();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (int) (myId ^ (myId >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    ChannelRefCounted other = (ChannelRefCounted) obj;
    if (myId != other.myId)
      return false;
    return true;
  }

  @Override
  public String toString() {
    if(resource == null) {
      return "ChannelRefCounted [myId=" + myId + ", ref="+getRefcount()+"]";
    } else {
      return "ChannelRefCounted [myId=" + myId + ", ref="+getRefcount()+", channel="+resource.remoteAddress()+"]";
    }
  }
}
