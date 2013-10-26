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

package org.ploy.search.function.filter;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.ploy.search.CollectorFilterable;
import org.ploy.search.CollectorFilterablePlugin;

public class RangeCollectorFilterablePlugin extends CollectorFilterablePlugin {

  public static final String NAME = "range";

  @Override
  public CollectorFilterable createCollectorFilterable(String qstr,
      SolrParams localParams, SolrParams params, SolrQueryRequest req,
      ValueSource valueSource, String valueStr) throws SyntaxError {

    ValueSourceRangeFilter rf = null;
    if (valueStr == null) {
      String l = localParams.get("l");
      String u = localParams.get("u");
      boolean includeLower = localParams.getBool("incl",true);
      boolean includeUpper = localParams.getBool("incu",true);
      rf = new ValueSourceRangeFilter(valueSource, l, u, includeLower, includeUpper);
    } else {
      verifyValueStr(valueStr, valueSource);
      rf = parseRange(valueSource, valueStr);
    }

    return new RangeCollectorFilterable(rf);
  }

  @Override
  public String getName() {
    return NAME;
  }

}
