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

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.PostFilter;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.ploy.search.function.ValueSourceCollectorFilter;

/**
 * use {@link PostFilter} and {@link FilterCollector} filter docId
 */
public class CollectorFilterQuery extends SolrConstantScoreQuery implements PostFilter {

	final ValueSourceCollectorFilter valueSourceFilter;

  public CollectorFilterQuery(ValueSourceCollectorFilter valueSourceFilter) {
    super(valueSourceFilter);
    super.setCache(false);
    super.setCost(120);
    this.valueSourceFilter = valueSourceFilter;
	}

	@Override
	public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
		@SuppressWarnings("rawtypes")
		Map fcontext = ValueSource.newContext(searcher);
		return new FilterCollector(valueSourceFilter.getCollectorFilterable(), fcontext);
	}

  @Override
  public void setCache(boolean cache) {
    if (cache) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          CollectorFilterQuery.class.getSimpleName()
              + " not support 'cache=true'");
    }
    super.setCache(cache);
  }
}
