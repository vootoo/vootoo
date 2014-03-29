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

package org.vootoo.search;

import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class CollectorFilterQParserPluginTest extends AbstractSolrTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml", getFile("solr/collection1").getParent(),
        "post_filter_core");
  }

  @Test
  public void test_collect_filter_in() {
    assertU(adoc("id", "1", "my_int", "0"));
    assertU(adoc("id", "2", "my_int", "2"));
    assertU(commit());
    assertU(adoc("id", "3", "my_int", "3"));
    assertU(adoc("id", "4", "my_int", "4"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", "*:*"), "//*[@numFound='4']",
        "//float[@name='score']='1.0'",
        "//result/doc[1]/int[@name='id'][.='1']",
        "//result/doc[2]/int[@name='id'][.='2']",
        "//result/doc[3]/int[@name='id'][.='3']",
        "//result/doc[4]/int[@name='id'][.='4']");

    assertQ(
        req("fl", "*,score", "q", "*:*", "fq", "{!cf name=in}my_int:(2, 3)"),
        "//*[@numFound='2']",
        "//float[@name='score']='1.0'",
        "//result/doc[1]/int[@name='id'][.='2']",
        "//result/doc[2]/int[@name='id'][.='3']");

    assertQ(req("fl", "*,score", "q", "{!cf name=in}my_int:4"),
        "//*[@numFound='1']", "//float[@name='score']='1.0'",
        "//result/doc[1]/int[@name='id'][.='4']");

    assertQ(req("q", "id:[1 TO 3]", "fq", "{!cf name=in}my_int:(3,4)"),
        "//*[@numFound='1']",
        "//result/doc[1]/int[@name='id'][.='3']");

    assertQ(req("fl", "*,score", "q", "{!cf name=in not=true}my_int:(0,4)"),
        "//*[@numFound='2']", "//float[@name='score']='1.0'",
        "//result/doc[1]/int[@name='id'][.='2']",
        "//result/doc[2]/int[@name='id'][.='3']");
  }

  @Test
  public void test_collect_filter_range() {
    assertU(adoc("id", "1", "my_int", "0"));
    assertU(adoc("id", "2", "my_int", "2"));
    assertU(adoc("id", "3", "my_int", "3"));
    assertU(adoc("id", "4", "my_int", "4"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", "*:*"), "//*[@numFound='4']",
        "//float[@name='score']='1.0'",
        "//result/doc[1]/int[@name='id'][.='1']",
        "//result/doc[2]/int[@name='id'][.='2']",
        "//result/doc[3]/int[@name='id'][.='3']",
        "//result/doc[4]/int[@name='id'][.='4']");

    assertQ(
        req("fl", "*,score", "q", "*:*", "fq",
            "{!cf name=range}my_int:[2 TO 3]"), "//*[@numFound='2']",
        "//float[@name='score']='1.0'",
        "//result/doc[1]/int[@name='id'][.='2']",
        "//result/doc[2]/int[@name='id'][.='3']");

    assertQ(req("fl", "*,score", "q", "{!cf name=range}my_int:(3 TO 4]"),
        "//*[@numFound='1']", "//float[@name='score']='1.0'",
        "//result/doc[1]/int[@name='id'][.='4']");

    assertQ(req("q", "id:3", "fq", "{!cf name=range}my_int:[3 TO 4]"),
        "//*[@numFound='1']", "//result/doc[1]/int[@name='id'][.='3']");

    assertQ(
        req("fl", "*,score", "q", "{!cf name=range not=true}my_int:[2 TO 3]"),
        "//*[@numFound='2']", "//float[@name='score']='1.0'",
        "//result/doc[1]/int[@name='id'][.='1']",
        "//result/doc[2]/int[@name='id'][.='4']");
  }
}
