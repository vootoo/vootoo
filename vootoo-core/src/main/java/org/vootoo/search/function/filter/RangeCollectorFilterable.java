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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.vootoo.search.CollectorFilterable;

import java.io.IOException;
import java.util.Map;

/**
 * range collector filter
 *
 * @see ValueSourceRangeFilter
 */
public class RangeCollectorFilterable extends CollectorFilterable {

  protected final ValueSourceRangeFilter valueSourceRangeFilter;

  protected ValueSourceScorer rangeScorer;

  public RangeCollectorFilterable(ValueSourceRangeFilter valueSourceRangeFilter) {
    this.valueSourceRangeFilter = valueSourceRangeFilter;
  }

  public ValueSource getValueSource() {
    return valueSourceRangeFilter.getValueSource();
  }

  public String getLowerVal() {
    return valueSourceRangeFilter.getLowerVal();
  }

  public String getUpperVal() {
    return valueSourceRangeFilter.getUpperVal();
  }

  public boolean isIncludeLower() {
    return valueSourceRangeFilter.isIncludeLower();
  }

  public boolean isIncludeUpper() {
    return valueSourceRangeFilter.isIncludeUpper();
  }

  @Override
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append("range(");
    sb.append(getValueSource().description());
    sb.append("):");
    sb.append(isIncludeLower() ? '[' : '(');
    sb.append(getLowerVal() == null ? "*" : getLowerVal());
    sb.append(" TO ");
    sb.append(getUpperVal() == null ? "*" : getUpperVal());
    sb.append(isIncludeUpper() ? ']' : ')');
    return sb.toString();
  }

  @Override
  public boolean matches(int doc) {
    if(rangeScorer != null) {
      return rangeScorer.matches(doc);
    }
    return true;
  }

  @Override
  public void doSetNextReader(@SuppressWarnings("rawtypes") Map context, LeafReaderContext readerContext)
      throws IOException {
    FunctionValues values = getValueSource().getValues(context, readerContext);
    rangeScorer = values.getRangeScorer(readerContext,
        getLowerVal(), getUpperVal(), isIncludeLower(), isIncludeUpper());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime
        * result
        + ((valueSourceRangeFilter == null) ? 0 : valueSourceRangeFilter
            .hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    RangeCollectorFilterable other = (RangeCollectorFilterable) obj;
    if (valueSourceRangeFilter == null) {
      if (other.valueSourceRangeFilter != null) return false;
    } else if (!valueSourceRangeFilter.equals(other.valueSourceRangeFilter)) return false;
    return true;
  }

}
