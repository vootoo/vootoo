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

import java.util.List;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SyntaxError;
import org.vootoo.search.CollectorFilterable;
import org.vootoo.search.CollectorFilterablePlugin;

public class InCollectorFilterablePlugin extends CollectorFilterablePlugin {

  public static final String NAME = "in";

  @Override
  public CollectorFilterable createCollectorFilterable(String qstr,
      SolrParams localParams, SolrParams params, SolrQueryRequest req,
      ValueSource valueSource, String valueStr) throws SyntaxError {

    verifyValueStr(valueStr, valueSource);

    List<String> lvs = parseMultiValue(valueStr, null, true);
    String[] inValues = lvs.toArray(new String[lvs.size()]);
    return new InCollectorFilterable(valueSource, inValues);
  }

  @Override
  public String getName() {
    return NAME;
  }

}