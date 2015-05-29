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

package org.vootoo.server.servlet;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.server.netty.SolrNettyServer;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * start and destroy netty server
 *
 * @author chenlb on 2015-05-22 10:12.
 */
public class VootooSolrDispatchFilter extends SolrDispatchFilter {
  private static final Logger logger = LoggerFactory.getLogger(VootooSolrDispatchFilter.class);

  private static final String NETTY_PROPERTIES = "netty.properties";

  protected volatile SolrNettyServer nettyServer;

  @Override
  public void init(FilterConfig config) throws ServletException {
    super.init(config);
    // init netty server
    initNettyServer(getCores());
  }

  @Override
  public void destroy() {
    // destroy netty server
    destoryNettyServer();
    super.destroy();
  }

  protected void initNettyServer(CoreContainer cores) {
    String solrHome = cores.getSolrHome();
    //load netty properties
    Properties nettyP = new Properties();
    File nettyProp = new File(solrHome, NETTY_PROPERTIES);
    if(nettyProp != null && nettyProp.isFile() && nettyProp.canRead()) {
      try {
        nettyP.load(new FileInputStream(nettyProp));
      } catch (IOException e) {
        logger.warn("loading from="+nettyProp.getAbsolutePath()+" is fail, ignore netty properties", e);
      }
    }

    //TODO start netty
    nettyServer = new SolrNettyServer(getCores(), Integer.parseInt(nettyP.getProperty("port", "8983")));
    try {
      nettyServer.startServer(false);
    } catch (Exception e) {
      e.printStackTrace();
      nettyServer = null;
    }
  }

  protected void destoryNettyServer() {
    if(nettyServer != null) {
      try {
        nettyServer.stopServer();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
