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

package org.vootoo.client.netty;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author chenlb on 2015-06-10 19:48.
 */
public class NettySolrClientDemo {

  NettySolrClient solrClient;

  @Before
  public void start_netty() {
    solrClient = new NettySolrClient("localhost", 8001);
  }

  @After
  public void stop_netty() {
    if(solrClient != null) {
      solrClient.shutdown();
    }
  }

  @Test
  public void test_query() {
    SolrQuery q = new SolrQuery("*:*");

    QueryResponse response = null;
    try {
      //response = solrClient.query(q);
      response = solrClient.query("demo", q);
    } catch (Throwable e) {
      e.printStackTrace();
    }

    System.out.println(response);
  }
}
