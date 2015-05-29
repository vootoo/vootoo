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

import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author chenlb on 2015-05-25 16:54.
 */
public interface ResponseSetter<T> {

  void setContentType(String contentType);

  void setStatus(int status);

  OutputStream getOutputStream();

  void sendError(int code, Throwable ex);

  /**
   * after writeQueryResponse call this method
   */
  void writeQueryResponseComplete(SolrQueryResponse solrQueryResponse);

  SolrQueryResponse getSolrQueryResponse();

  T buildProtocolResponse();
}
