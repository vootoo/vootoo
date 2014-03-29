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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.vootoo.search.ValueSourceCollectorFilterable;

/**
 * link sql <code>in=(x, y, z)</code>
 */
public class InCollectorFilterable extends ValueSourceCollectorFilterable {

  private final String[] inValues;

	private Set<Object> inObjValues;

	private FunctionValues functionValues;

	public InCollectorFilterable(ValueSource valueSource, String[] inValues) {
	  super(valueSource);
		this.inValues = inValues;
	}

	@Override
	public String description() {
		return "in("+valueSource.description()+"):" + Arrays.toString(inValues);
	}

	@Override
	public boolean matches(int doc) {
		Object obj = functionValues.objectVal(doc);
		if(inObjValues == null) {
			inObjValues = new HashSet<Object>(inValues.length * 2);
			for(Object objValue : parseValue(inValues, obj)) {
				inObjValues.add(objValue);
			}
		}
		return inObjValues.contains(obj);
	}

	@Override
	public void setNextReader(@SuppressWarnings("rawtypes") Map context, AtomicReaderContext readerContext) throws IOException {
		functionValues = valueSource.getValues(context, readerContext);
	}

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Arrays.hashCode(inValues);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    InCollectorFilterable other = (InCollectorFilterable) obj;
    if (!Arrays.equals(inValues, other.inValues)) return false;
    return true;
  }

}
