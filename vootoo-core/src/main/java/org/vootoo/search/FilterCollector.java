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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.search.DelegatingCollector;

import java.io.IOException;
import java.util.Map;

public class FilterCollector extends DelegatingCollector {

	protected final CollectorFilterable filter;
	@SuppressWarnings("rawtypes")
	protected final Map fcontext;
	int maxdoc;

	public FilterCollector(CollectorFilterable filterableCollector, @SuppressWarnings("rawtypes") Map fcontext) {
		this.filter = filterableCollector;
		this.fcontext = fcontext;
	}

	@Override
	public void collect(int doc) throws IOException {
		if(doc < maxdoc && filter.matches(doc)) {
			leafDelegate.collect(doc);
		}
	}

	@Override
	public void doSetNextReader(LeafReaderContext context) throws IOException {
		maxdoc = context.reader().maxDoc();
		filter.doSetNextReader(fcontext, context);
		super.doSetNextReader(context);
	}

}