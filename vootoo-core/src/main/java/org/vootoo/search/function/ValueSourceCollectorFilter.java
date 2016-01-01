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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;
import org.apache.solr.search.SolrFilter;
import org.vootoo.search.CollectorFilterable;
import org.vootoo.search.ValueSourceCollectorFilterable;

import java.io.IOException;
import java.util.Map;

/**
 * CollectorFilter over a ValueSource
 */
public class ValueSourceCollectorFilter extends SolrFilter {

	private final ValueSource valueSource;
	private final CollectorFilterable collectorFilterable;

	public ValueSourceCollectorFilter(ValueSource valueSource, CollectorFilterable collectorFilterable) {
		this.valueSource = valueSource;
		this.collectorFilterable = collectorFilterable;
	}

	public ValueSourceCollectorFilter(ValueSourceCollectorFilterable valueSourceCollectorFilterable) {
	  this(valueSourceCollectorFilterable.getValueSource(), valueSourceCollectorFilterable);
	}

	@Override
	public void createWeight(@SuppressWarnings("rawtypes") Map context, IndexSearcher searcher) throws IOException {
		valueSource.createWeight(context, searcher);
	}

	@Override
	public DocIdSet getDocIdSet(@SuppressWarnings("rawtypes") final Map context, final LeafReaderContext readerContext, Bits acceptDocs) throws IOException {
		collectorFilterable.doSetNextReader(context, readerContext);
		return BitsFilteredDocIdSet.wrap(new DocIdSet() {
			@Override
			public long ramBytesUsed() {
				return 0;
			}

			@Override
			public DocIdSetIterator iterator() throws IOException {
				return new ValueSourceScorer(readerContext.reader(), valueSource.getValues(context, readerContext)) {

					@Override
					public boolean matches(int doc) {
						return collectorFilterable.matches(doc);
					}

				};
			}

			@Override
			public Bits bits() {
				return null; // don't use random access
			}
		}, acceptDocs);
	}

	public String toString(String field) {
		StringBuilder sb = new StringBuilder();
		sb.append("vsfilter(");
		sb.append(valueSource).append(",");
		sb.append(collectorFilterable.description());
		sb.append(")");
		return sb.toString();
	}

	public ValueSource getValueSource() {
		return valueSource;
	}

	public CollectorFilterable getCollectorFilterable() {
		return collectorFilterable;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((collectorFilterable == null) ? 0 : collectorFilterable.hashCode());
		result = prime * result + ((valueSource == null) ? 0 : valueSource.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(this == obj)
			return true;
		if(obj == null)
			return false;
		if(getClass() != obj.getClass())
			return false;
		ValueSourceCollectorFilter other = (ValueSourceCollectorFilter) obj;
		if(collectorFilterable == null) {
			if(other.collectorFilterable != null)
				return false;
		} else if(!collectorFilterable.equals(other.collectorFilterable))
			return false;
		if(valueSource == null) {
			if(other.valueSource != null)
				return false;
		} else if(!valueSource.equals(other.valueSource))
			return false;
		return true;
	}

}
