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

package org.vootoo.core;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.ConfigSolr;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

/**
 */
public class CoreContainerUtil {

  static final Logger log = LoggerFactory.getLogger(CoreContainerUtil.class);

  /**
   * @param solrHome is null use {@link SolrResourceLoader#locateSolrHome()}
   */
  public static CoreContainer createCoreContainer(String solrHome) {
    SolrResourceLoader loader = null;
    if(solrHome != null) {
      solrHome = SolrResourceLoader.normalizeDir(solrHome);
      loader = new SolrResourceLoader(solrHome);
    } else {
      loader = new SolrResourceLoader(SolrResourceLoader.locateSolrHome());
    }
    ConfigSolr config = loadConfigSolr(loader);
    CoreContainer cores = new CoreContainer(loader, config);
    cores.load();
    return cores;
  }

  private static ConfigSolr loadConfigSolr(SolrResourceLoader loader) {

    String solrxmlLocation = System.getProperty("solr.solrxml.location", "solrhome");

    if (solrxmlLocation == null || "solrhome".equalsIgnoreCase(solrxmlLocation))
      return ConfigSolr.fromSolrHome(loader, loader.getInstanceDir());

    if ("zookeeper".equalsIgnoreCase(solrxmlLocation)) {
      String zkHost = System.getProperty("zkHost");
      log.info("Trying to read solr.xml from " + zkHost);
      if (StringUtils.isEmpty(zkHost))
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Could not load solr.xml from zookeeper: zkHost system property not set");
      SolrZkClient zkClient = new SolrZkClient(zkHost, 30000);
      try {
        if (!zkClient.exists("/solr.xml", true))
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not load solr.xml from zookeeper: node not found");
        byte[] data = zkClient.getData("/solr.xml", null, null, true);
        return ConfigSolr.fromInputStream(loader, new ByteArrayInputStream(data));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not load solr.xml from zookeeper", e);
      } finally {
        zkClient.close();
      }
    }

    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
        "Bad solr.solrxml.location set: " + solrxmlLocation + " - should be 'solrhome' or 'zookeeper'");
  }
}
