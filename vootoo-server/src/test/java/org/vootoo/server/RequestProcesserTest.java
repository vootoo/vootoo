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

import junit.framework.Assert;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.client.netty.util.ProtobufUtil;
import org.vootoo.server.netty.ProtobufResponseSetter;

import java.io.OutputStream;
import java.util.Collection;
import java.util.UUID;

/**
 * @author chenlb on 2015-05-28 16:10.
 */
public class RequestProcesserTest extends SolrTestCaseJ4 {

  private static final Logger logger = LoggerFactory.getLogger(RequestProcesserTest.class);

  private static class MemReqestGetter implements RequestGetter {

    SolrQuery query;

    public MemReqestGetter(String id) {
      query = new SolrQuery("*:*");
      query.set("_timeout_", 2000);
      query.set("indent", "on");
      query.set("wt", "xml");
    }

    @Override
    public SolrParams getSolrParams() {
      return query;
    }

    @Override
    public Collection<ContentStream> getContentStreams() {
      return null;
    }

    @Override
    public String getCollection() {
      return "collection1";
    }

    @Override
    public String getPath() {
      return "/select";
    }
  }

  private static class MemResponseSetter extends ProtobufResponseSetter {
    public MemResponseSetter() {
      super(1);
    }

    @Override
    public void setContentType(String contentType) {
      logger.debug("setContentType={}", contentType);
      super.setContentType(contentType);
    }

    @Override
    public void setStatus(int status) {
      logger.debug("setStatus={}", status);
      super.setStatus(status);
    }

    @Override
    public void sendError(int code, Throwable ex) {
      logger.debug("sendError=" + code, ex);
      super.sendError(code, ex);
    }

    @Override
    public void writeQueryResponseComplete(SolrQueryResponse solrQueryResponse) {
      logger.debug("writeQueryResponseComplete, header={}, values={}", solrQueryResponse.getResponseHeader(), solrQueryResponse.getValues());
      super.writeQueryResponseComplete(solrQueryResponse);
    }

    @Override
    public OutputStream getOutputStream() {
      logger.debug("getOutputStream for write QueryResponse");
      return super.getOutputStream();
    }

    @Override
    public SolrProtocol.SolrResponse buildProtocolResponse() {
      logger.debug("buildProtocolResponse");
      return super.buildProtocolResponse();
    }
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  protected String addTestDoc() {
    String id = UUID.randomUUID().toString();
    assertU(adoc("id", id));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", "id:\""+id+"\""), "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='"+id+"']");

    return id;
  }

  protected void assertIdResult(QueryResponse queryResponse, String idValue) {
    SolrDocumentList results = queryResponse.getResults();
    Assert.assertEquals(results.getNumFound(), 1);
    Assert.assertEquals(idValue, results.get(0).getFieldValue("id"));
  }

  protected QueryResponse processResponse(SolrProtocol.SolrResponse protocolResponse) {
    String charset = ProtobufUtil.getResponseBodyCharset(protocolResponse);

    /*
    byte[] bytes = ByteStreams.toByteArray(ProtobufUtil.getSolrResponseInputStream(protocolResponse));
    System.out.println();
    System.out.println(new String(bytes, charset));
    */

    XMLResponseParser parser = new XMLResponseParser();

    NamedList<Object> namedList = parser.processResponse(ProtobufUtil.getSolrResponseInputStream(protocolResponse), charset);
    QueryResponse queryResponse = new QueryResponse();
    queryResponse.setResponse(namedList);

    /*
    System.out.println();
    System.out.println(namedList);
    */

    return queryResponse;
  }

  @Test
  public void testHandleRequest() throws Exception {
    String id = addTestDoc();

    ProtobufResponseSetter responeSetter = new MemResponseSetter();

    RequestProcesser requestProcesser = new RequestProcesser(h.getCoreContainer(), responeSetter);

    //do handle request
    requestProcesser.handleRequest(new MemReqestGetter(id));

    assertIdResult(processResponse(responeSetter.buildProtocolResponse()), id);
  }
}