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

package org.vootoo.logging.logback;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.LogWatcherConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogbackWatcherTest {
  private LogWatcherConfig config;
  private SolrResourceLoader loader;

  @Before
  public void setUp() {
    config = new LogWatcherConfig(true, LogbackWatcher.class.getName(), null, 50);
    loader = new SolrResourceLoader(Paths.get("."), Runtime.getRuntime().getClass().getClassLoader(), null);
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testLogbackWatcher() {

    Logger log = LoggerFactory.getLogger("testlogger");
    LogWatcher watcher = LogWatcher.newRegisteredLogWatcher(config, loader);

    assertEquals(watcher.getLastEvent(), -1);

    log.warn("This is a test message");

    assertTrue(watcher.getLastEvent() > -1);

    SolrDocumentList events = watcher.getHistory(-1, new AtomicBoolean());
    assertEquals(events.size(), 1);

    SolrDocument event = events.get(0);
    assertEquals(event.get("logger"), "testlogger");
    assertEquals(event.get("message"), "This is a test message");


    //test set watcherThreshold
    watcher.setThreshold("info");
    long last = watcher.getLastEvent();
    sleep(5);

    log.info("test info message");

    assertTrue(watcher.getLastEvent() > last);

    events = watcher.getHistory(last, new AtomicBoolean());
    assertEquals(events.size(), 1);

    event = events.get(0);
    assertEquals(event.get("logger"), "testlogger");//level
    assertEquals(event.get("message"), "test info message");
    assertTrue("info".equalsIgnoreCase(event.get("level").toString()));

    watcher.setThreshold("warn");
    last = watcher.getLastEvent();
    sleep(5);

    log.info("test another info message");

    assertTrue(watcher.getLastEvent() == last);
  }
}
