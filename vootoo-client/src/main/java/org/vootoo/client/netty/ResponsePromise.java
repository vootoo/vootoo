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

import io.netty.util.concurrent.Promise;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.protocol.SolrProtocol;

/**
 * @author chenlb on 2015-06-13 13:21.
 */
public class ResponsePromise {
  private static final Logger logger = LoggerFactory.getLogger(ResponsePromise.class);

  private long rid;
  private Promise<SolrProtocol.SolrResponse> responsePromise;

  public ResponsePromise(long rid) {
    this.rid = rid;
  }

  public void setResponsePromise(Promise<SolrProtocol.SolrResponse> responsePromise) {
    this.responsePromise = responsePromise;
  }

  public long getRid() {
    return rid;
  }

  void setRid(long rid) {
    this.rid = rid;
  }

  public void receiveResponse(SolrProtocol.SolrResponse solrResponse) {
    assert responsePromise != null;

    responsePromise.setSuccess(solrResponse);
  }

  public void setFailure(Throwable cause) {
    responsePromise.setFailure(cause);
  }

  public SolrProtocol.SolrResponse waitResult(long timeout) {
    assert responsePromise != null;

    responsePromise.awaitUninterruptibly(timeout);

    if(responsePromise.isDone()) {
      if(responsePromise.isSuccess()) {
        return responsePromise.getNow();
      } else {
        //TODO throw ?
        logger.warn("rid=["+rid+"] response not success", responsePromise.cause());
      }
    }
    return null;
  }
}
