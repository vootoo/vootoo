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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public interface ChannelPool {
  /**
   * 使用完后要调用  ChannelRefCounted.decref();
   * @return
   * @throws java.io.IOException 连接失败
   */
  ChannelRefCounted getChannel(int maxTry) throws IOException;

  String channelHost();

  int channelPort();

  long getConnectTimeout();

  void setConnectTimeout(long connectTimeout);

  void decref();

  void close();

  /**
   * Channel 序号定位
   */
  public static interface NumLocator {

    /**
     * 定位序号。
     * @return < totalSize 的值。用于选择 Channel
     */
    int locate(int totalSize);
  }

  /**
   * 轮询定位
   */
  public static class RobinNumLocator implements NumLocator {

    private final AtomicInteger idxCursor = new AtomicInteger();

    @Override
    public int locate(int totalSize) {
      return Math.abs(idxCursor.getAndIncrement()%totalSize);
    }

    public void resetCursor(int cursor) {
      idxCursor.set(cursor);
    }
  }

  /**
   * 随机定位
   */
  public static class RandomNumLocator implements NumLocator {

    private final ThreadLocal<Random> randomLocal = new ThreadLocal<Random>() {

      @Override
      protected Random initialValue() {
        return new Random();
      }

    };

    @Override
    public int locate(int totalSize) {
      return randomLocal.get().nextInt(totalSize);
    }
  }
}
