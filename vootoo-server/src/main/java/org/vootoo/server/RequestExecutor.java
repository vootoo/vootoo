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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @author chenlb on 2015-05-22 11:35.
 */
public class RequestExecutor {
  private static final Logger logger = LoggerFactory.getLogger(RequestExecutor.class);

  private final ExecutorConfig config;
  private final ThreadPoolExecutor requestExecutor;
  private ListeningExecutorService executorService;

  public RequestExecutor(String name, int threadNum, int maxWaitTask) {
    this(ExecutorConfig.createDefault().setName(name).setThreadNum(threadNum).setMaxWaitTask(maxWaitTask));
  }

  public RequestExecutor(ExecutorConfig config) {
    this.config = config;
    requestExecutor = new ThreadPoolExecutor(config.getThreadNum(), config.getThreadNum(), config.getThreadKeepAliveMinute(), TimeUnit.MINUTES,
        new ArrayBlockingQueue<Runnable>(config.getMaxWaitTask()), new DefaultSolrThreadFactory(config.getName()));
    executorService = MoreExecutors.listeningDecorator(requestExecutor);
  }

  public void changeThreadNum(int newThreadNum) {
    if(requestExecutor.getMaximumPoolSize() != newThreadNum) {
      logger.info("{} RequestExecutor thread number expect from {} to {}", new Object[] {
          config.getName(), config.getThreadNum(), newThreadNum
      });

      if(requestExecutor.getMaximumPoolSize() > newThreadNum) {// expect zoom out
        requestExecutor.setCorePoolSize(newThreadNum);
        requestExecutor.setMaximumPoolSize(newThreadNum);
      } else if(requestExecutor.getMaximumPoolSize() < newThreadNum) {// expect zoom in
        requestExecutor.setMaximumPoolSize(newThreadNum);
        requestExecutor.setCorePoolSize(newThreadNum);
      }

      config.setThreadNum(newThreadNum);
    } else {
      logger.info("{} RequestExecutor thread number expect={} is not change", new Object[] {
          config.getName(), newThreadNum
      });
    }

  }

  public boolean shutdownAndAwaitTermination(long timeout, TimeUnit unit) {
    return MoreExecutors.shutdownAndAwaitTermination(executorService, timeout, unit);
  }

  public <T> ListenableFuture<T> submitTask(Callable<T> task) {
    return executorService.submit(task);
  }

  public <T> ListenableFuture<T> submitTask(Runnable task, T result) {
    return executorService.submit(task, result);
  }
}
