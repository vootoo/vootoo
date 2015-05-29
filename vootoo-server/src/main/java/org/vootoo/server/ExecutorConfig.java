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

package org.vootoo.server;

/**
 * @author chenlb on 2015-05-23 17:12.
 */
public class ExecutorConfig {

  private String name;
  private int threadNum;
  /**
   * thread keep alive minute, default 5 min.
   */
  private int threadKeepAliveMinute = 5;

  /**
   * max wait task, default 1k
   */
  private int maxWaitTask = 1000;

  public static ExecutorConfig createDefault() {
    ExecutorConfig config = new ExecutorConfig();
    config.setName("request-executor");
    config.setMaxWaitTask(1000);
    config.setThreadKeepAliveMinute(5);
    config.setThreadNum(80);
    return config;
  }

  public String getName() {
    return name;
  }

  public ExecutorConfig setName(String name) {
    this.name = name;
    return this;
  }

  public int getThreadNum() {
    return threadNum;
  }

  public ExecutorConfig setThreadNum(int threadNum) {
    this.threadNum = threadNum;
    return this;
  }

  public int getThreadKeepAliveMinute() {
    return threadKeepAliveMinute;
  }

  public ExecutorConfig setThreadKeepAliveMinute(int threadKeepAliveMinute) {
    this.threadKeepAliveMinute = threadKeepAliveMinute;
    return this;
  }

  public int getMaxWaitTask() {
    return maxWaitTask;
  }

  public ExecutorConfig setMaxWaitTask(int maxWaitTask) {
    this.maxWaitTask = maxWaitTask;
    return this;
  }
}
