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

package org.vootoo.search.function.filter;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SyntaxError;
import org.vootoo.search.CollectorFilterable;
import org.vootoo.search.CollectorFilterablePlugin;

import java.util.List;

/**
 * fq={!cf name=bit}bit:(0b01100)
 */
public class BitCollectorFilterablePlugin extends CollectorFilterablePlugin {

  public static final String NAME = "bit";

  @Override
  public CollectorFilterable createCollectorFilterable(String qstr,
      SolrParams localParams, SolrParams params, SolrQueryRequest req,
      ValueSource valueSource, String valueStr) throws SyntaxError {

    verifyValueStr(valueStr, valueSource);

    List<String> lvs = parseMultiValue(valueStr, null, true);
    long queryBit = 0;
    // queryBit = a | b | c ...
    for(String lv : lvs) {
      Long qv = null;
      try {
        qv = parseLongExt(lv);
      } catch (NumberFormatException e) {
        throw new SyntaxError(lv+" can't parse long", e);
      }
      if(qv != null) {
        queryBit |= qv.longValue();
      }
    }
    return new BitCollectorFilterable(valueSource, queryBit);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
