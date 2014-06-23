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

import java.util.Collection;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

/**
 */
public class SolrRequest {

  private long rid;
  private String coreName;
  private String path;
  private SolrParams solrParams;
  private Collection<ContentStream> contentStreams;
  
  public SolrParams getSolrParams() {
    return solrParams;
  }
  
  public void setSolrParams(SolrParams solrParams) {
    this.solrParams = solrParams;
  }
  
  public Collection<ContentStream> getContentStreams() {
    return contentStreams;
  }
  
  public void setContentStreams(Collection<ContentStream> contentStreams) {
    this.contentStreams = contentStreams;
  }
  
  public String getCoreName() {
    return coreName == null ? "" : coreName;
  }
  
  public String getPath() {
    return path;
  }

  public long getRid() {
    return rid;
  }
  
  public void setRid(long rid) {
    this.rid = rid;
  }
  
  public void setCoreName(String coreName) {
    this.coreName = coreName;
  }
  
  public void setPath(String path) {
    this.path = path;
  }
}
