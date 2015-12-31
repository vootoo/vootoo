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

package org.vootoo.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.util.TestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.NamedCoresLocator;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;

public class CrossCoreFieldTest {

  private static final Logger logger = LoggerFactory.getLogger(CrossCoreFieldTest.class);
  static CoreContainer coreContainer;

  static String MAIN_CORE = "cross_core_main";
  static String SUB_CORE = "cross_core_sub";

  @BeforeClass
  public static void beforeClass() {
    coreContainer = new CoreContainer(TestHarness.buildTestNodeConfig(new SolrResourceLoader(SolrTestCaseJ4.TEST_HOME())),
        new Properties(),
        new NamedCoresLocator(MAIN_CORE, SUB_CORE));
    coreContainer.load();
  }

  @AfterClass
  public static void afterClass() {
    if(coreContainer != null) {
      coreContainer.shutdown();
    }
  }

  private void addDoc(String coreName, AddUpdateCommand doc) throws IOException {
    SolrCore core = coreContainer.getCore(coreName);

   try {
     core.getUpdateHandler().addDoc(doc);
   } finally {
     core.close();
   }
  }

  private void commit(String coreName) throws IOException {
    SolrCore core = coreContainer.getCore(coreName);

    try {
      core.getUpdateHandler().commit(new CommitUpdateCommand(null, false));

    } finally {
      core.close();
    }
  }

  public LocalSolrQueryRequest makeRequest(String handler, SolrCore core, int start, int limit, String ... q) {
    if (q.length==1) {
      return new LocalSolrQueryRequest(core,
          q[0], handler, start, limit, LocalSolrQueryRequest.emptyArgs);
    }
    if (q.length%2 != 0) {
      throw new RuntimeException("The length of the string array (query arguments) needs to be even");
    }
    Map.Entry<String, String> [] entries = new NamedList.NamedListEntry[q.length / 2];
    for (int i = 0; i < q.length; i += 2) {
      entries[i/2] = new NamedList.NamedListEntry<>(q[i], q[i+1]);
    }
    NamedList nl = new NamedList(entries);
    if(nl.get("wt" ) == null) nl.add("wt","xml");
    return new LocalSolrQueryRequest(core, nl);
  }

  public String query(String coreName, String ... query) throws Exception {
    String handler = "/select";
    SolrQueryRequest req = null;
    try(SolrCore core = coreContainer.getCore(coreName)) {
      req = makeRequest(handler, core, 0, 10, query);
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      core.execute(core.getRequestHandler(handler),req,rsp);
      if (rsp.getException() != null) {
        throw rsp.getException();
      }
      StringWriter sw = new StringWriter(32000);
      QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
      responseWriter.write(sw, req, rsp);
      return sw.toString();
    } finally {
      if(req != null) {
        req.close();
      }
      SolrRequestInfo.clearRequestInfo();
    }
  }

  public String update(String coreName, String xml) {
    try (SolrCore core = coreContainer.getCore(coreName)) {
      DirectSolrConnection connection = new DirectSolrConnection(core);
      SolrRequestHandler handler = core.getRequestHandler("/update");
      // prefer the handler mapped to /update, but use our generic backup handler
      // if that lookup fails
      if (handler == null) {
        throw new Exception("not found update handler");
      }
      return connection.request(handler, null, xml);
    } catch (SolrException e) {
      throw (SolrException)e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  public static void assertQ(String response, String... tests) {
    try {
      String results = TestHarness.validateXPath(response, tests);

      if (null != results) {
        String msg = "REQUEST FAILED: xpath=" + results
            + "\n\txml response was: " + response
            + "\n\trequest was:";

        logger.error(msg);
        throw new RuntimeException(msg);
      }

    } catch (XPathExpressionException e1) {
      throw new RuntimeException("XPath is invalid", e1);
    } catch (Exception e2) {
      SolrException.log(logger,"REQUEST FAILED: ", e2);
      throw new RuntimeException("Exception during query", e2);
    }
  }

  @Test
  public void test_Sort_Cross_Core_ValueSource() throws Exception {

    update(MAIN_CORE, SolrTestCaseJ4.adoc("id", "1", "my_i", "100"));
    update(MAIN_CORE, SolrTestCaseJ4.adoc("id", "2", "my_i", "50"));
    update(MAIN_CORE, "<commit/>");

    assertQ(query(MAIN_CORE, "q", "*:*"),
        "//result/doc[1]/int[@name='my_i'][.='100']",
        "//result/doc[2]/int[@name='my_i'][.='50']"
    );

    update(SUB_CORE, SolrTestCaseJ4.adoc("id", "1", "other_i", "20"));
    update(SUB_CORE, SolrTestCaseJ4.adoc("id", "2", "other_i", "80"));
    update(SUB_CORE, "<commit/>");

    assertQ(query(SUB_CORE, "q", "*:*"),
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']"
    );

    assertQ(query(MAIN_CORE, "q", "*:*", "sort", "mul(_solr_"+SUB_CORE+".other_i, 2) desc"),
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='1']"
    );
  }

  @Test
  public void test_Return_Cross_Core_Field() {
    //TODO test cross core 'fl' by transform
    /*
        System.out.println(query(MAIN_CORE, "q", "*:*", "sort", "mul(_solr_"+SUB_CORE+".other_i, 2) desc",
        "fsv","true","indent","on",
        "fl", "*,other_i:field(_solr_"+SUB_CORE+".other_i)"));
     */
  }
}