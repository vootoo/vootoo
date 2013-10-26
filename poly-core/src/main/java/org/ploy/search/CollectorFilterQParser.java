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

package org.ploy.search;

import java.util.Map;

import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.ploy.search.function.ValueSourceCollectorFilter;

/**
 * cf QParser
 */
public class CollectorFilterQParser extends FunctionQParser {

  public static final String CF_NAME = "name";
  public static final String CF_NOT = "not";

  protected Map<String,CollectorFilterablePlugin> customPlugins = null;

  public CollectorFilterQParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req,
      Map<String,CollectorFilterablePlugin> customPlugins) {
    super(qstr, localParams, params, req);
    setParseMultipleSources(false);
    setParseToEnd(false);
    this.customPlugins = customPlugins;
  }

  public CollectorFilterablePlugin getCollectorFilterablePlugin(String name) {
    CollectorFilterablePlugin cfPlugin = CollectorFilterablePlugin.standardPlugins
        .get(name);
    if (cfPlugin == null && customPlugins != null) {
      cfPlugin = customPlugins.get(name);
    }
    return cfPlugin;
  }

  @Override
  public Query parse() throws SyntaxError {
    if (localParams == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          CollectorFilterQParserPlugin.NAME + " QParser miss localParams");
    }
    String name = localParams.get(CF_NAME);
    CollectorFilterablePlugin cfPlugin = getCollectorFilterablePlugin(name);
    if (cfPlugin == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          CollectorFilterQParserPlugin.NAME + " QParser not found '" + name
              + "' CollectorFilterablePlugin");
    }

    // parse func for vs
    FunctionQuery funQ = (FunctionQuery) super.parse();
    ValueSource vs = funQ.getValueSource();

    // try parse value
    String valueStr = null;
    int valueIndex = qstr.indexOf(":");
    if (valueIndex > 0 && valueIndex < qstr.length() - 1) {
      valueStr = qstr.substring(valueIndex + 1);
    }

    // create cf
    CollectorFilterable cf = cfPlugin.createCollectorFilterable(name,
        localParams, localParams, req, vs, valueStr);

    if (localParams.getBool(CF_NOT, false)) {
      // negative cf
      cf = new WrappedNotCollectorFilterable(cf);
    }

    return new CollectorFilterQuery(new ValueSourceCollectorFilter(vs, cf));
  }

}
