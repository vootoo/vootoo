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

package org.vootoo.search.function;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chenlb on 2016-12-24 10:34.
 */
public class RandomValueSourceParserTest extends AbstractSolrTestCase {
  private static final Logger logger = LoggerFactory.getLogger(RandomValueSourceParserTest.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml", getFile("solr/collection1").getParent(), "function_core");
  }

  private boolean assertNotSameSum(Object inputObj) {
    int sameSum = 0;
    for(int i=0; i<4; i++) {
      String err = JSONTestUtil.matchObj("response/docs/["+i+"]/id", inputObj, i+1, JSONTestUtil.DEFAULT_DELTA);
      if(err != null) {
        //System.out.println(err);
      } else {
        sameSum += 1;
      }
    }
    if(sameSum == 4) {
      logger.warn("sort rand maybe fail, sameSum="+sameSum);
    } else {
      logger.info("sort randed="+(sameSum < 4)+" sameSum="+sameSum);
    }
    //Assert.assertTrue("not rand, sameSum="+sameSum+", total=4", sameSum < 4);
    return sameSum < 4;
  }

  @Test
  public void test_rand_func() throws Exception {
    assertU(adoc("id", "1", "my_int", "0"));
    assertU(adoc("id", "2", "my_int", "2"));
    assertU(commit());
    assertU(adoc("id", "3", "my_int", "3"));
    assertU(adoc("id", "4", "my_int", "4"));
    assertU(commit());

    assertJQ(req("fl", "*,score", "q", "*:*"), "response/numFound==4",
        "response/docs/[0]/id==1",
        "response/docs/[1]/id==2",
        "response/docs/[2]/id==3",
        "response/docs/[3]/id==4"
        );

    assertQ(req("fl", "*,rand:rand(20)", "q", "*:*"),
        "//result/doc[1]/int[@name='rand'][.<20]",
        "//result/doc[2]/int[@name='rand'][.<20]",
        "//result/doc[3]/int[@name='rand'][.<20]",
        "//result/doc[4]/int[@name='rand'][.<20]"
        );

    int failSum = 0;
    for(int i=0; i<5; i++) {//随机5次应该不同
      boolean notSame = assertNotSameSum(ObjectBuilder.fromJSON(JQ(req("q","*:*", "sort", "rand() desc"))));
      if(!notSame) {
        failSum += 1;
      }
    }
    Assert.assertTrue(failSum < 2);
    //System.out.println(JQ(req("q","*:*", "fl", "*,rand:rand(20)", "sort", "rand() desc")));
    //System.out.println(JQ(req("q","*:*", "fl", "*,rand:rand(20)", "sort", "rand() desc")));

  }
}
