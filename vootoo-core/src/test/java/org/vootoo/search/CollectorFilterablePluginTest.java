package org.vootoo.search;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.junit.Assert;
import org.junit.Test;
import org.vootoo.search.CollectorFilterablePlugin;

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

public class CollectorFilterablePluginTest {

  private void assertMv(Map<String,String[]> mvalues, String spiltStr) {
    for (Map.Entry<String,String[]> ev : mvalues.entrySet()) {
      try {
        List<String> vv = CollectorFilterablePlugin.parseMultiValue(ev.getKey(), spiltStr,
            true);
        Assert.assertNotNull(vv);
        Assert.assertArrayEquals(vv.toArray(new String[0]), ev.getValue());
      } catch (SyntaxError e) {
        Assert.fail("parse v=" + ev.getKey() + " error, " + e.getMessage());
      }
    }
  }

  private void assertRange(String range, String min, String max, boolean incMin, boolean incMax, boolean error) {
    try {
      ValueSourceRangeFilter vsrf = CollectorFilterablePlugin.parseRange(null, range);
      if(error) {
        Assert.fail("parse v=" + range + " ok, but need error");
      }
      Assert.assertNotNull(vsrf);
      Assert.assertEquals(vsrf.getLowerVal(), min);
      Assert.assertEquals(vsrf.getUpperVal(), max);
      Assert.assertEquals(vsrf.isIncludeLower(), incMin);
      Assert.assertEquals(vsrf.isIncludeUpper(), incMax);
    } catch (SyntaxError e) {
      if(!error) {
        Assert.fail("parse v=" + range + " error, " + e.getMessage());
      }
    }

  }

  @Test
  public void testParseMultiValue() {
    // single value
    String[] values = {"1", "200", "10.34", "eng", "abc-_123", "1,2,3"};
    for (String v : values) {
      try {
        List<String> vv = CollectorFilterablePlugin.parseMultiValue(v, null, true);
        Assert.assertNotNull(vv);
        Assert.assertEquals(vv.size(), 1);
        Assert.assertEquals(vv.get(0), v);
      } catch (SyntaxError e) {
        Assert.fail("parse v=" + v + " error, " + e.getMessage());
      }
    }

    Map<String,String[]> mvalues = new HashMap<String,String[]>();
    mvalues.put("(1,2,3)", new String[] {"1", "2", "3"});
    mvalues.put(" (1, 2, 3)", new String[] {"1", "2", "3"});
    mvalues.put(" ( 1 , 2 , 3 ) ", new String[] {"1", "2", "3"});
    assertMv(mvalues, null);

    mvalues = new HashMap<String,String[]>();
    mvalues.put("(1#2#3,4)", new String[] {"1", "2", "3,4"});
    assertMv(mvalues, "#");
  }



  @Test
  public void testParseRange() {
    assertRange("[10 TO 20]", "10", "20", true, true, false);
    assertRange("(10 TO 20]", "10", "20", false, true, false);
    assertRange("(10 TO 20)", "10", "20", false, false, false);
    assertRange(" [ 10 TO 20 ] ", "10", "20", true, true, false);
    assertRange("[10 TO20]", "10", "20", false, false, true);
  }
}
