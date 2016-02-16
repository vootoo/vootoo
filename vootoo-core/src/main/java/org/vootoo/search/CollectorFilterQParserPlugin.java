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

import java.util.Map;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

/**
 * CollectorFilter ({@link org.apache.solr.search.PostFilter}). create {@link CollectorFilterQuery}(not use query cache)
 * <p/>
 * add solrconfig.xml<br/>
 * <code>&lt;queryParser name="cf" class="org.vootoo.search.CollectorFilterQParserPlugin"/&gt;</code><p/>
 * example:<p/>
 * <code>fq={!cf name=in}status:(-1, 2)</code><br/>
 * <code>fq={!cf name=in not=true}status:(3,4)</code><br/>
 * <code>fq={!cf name=range}price:[100 TO 500]</code><br/>
 * <code>fq={!cf name=range}log(page_view):[50 TO 120]</code><br/>
 * <code>fq={!cf name=range}geodist():[* TO 5]</code><br/>
 * <code>fq={!cf name=bit}bit_field:(0b01100)</code><br/>
 * <code>fq={!cf name=bit}bit_field:(0xa)</code><br/>
 * <code>fq={!cf name=bit}bit_field:(3)</code><br/>
 * <code>fq={!cf name=cbit}bit_field:(0b01100)</code><br/>
 */
public class CollectorFilterQParserPlugin extends QParserPlugin {

  public static final String NAME = "cf";

  protected Map<String,CollectorFilterablePlugin> customPlugins = null;

	@SuppressWarnings("rawtypes")
	@Override
	public void init(NamedList args) {
    // TODO init custom CollectorFilterablePlugins
	}

	@Override
	public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new CollectorFilterQParser(qstr, localParams, params, req,
        customPlugins);
	}

}
