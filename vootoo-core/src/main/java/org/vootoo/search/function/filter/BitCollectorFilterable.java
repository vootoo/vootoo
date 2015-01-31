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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.vootoo.search.ValueSourceCollectorFilterable;

import java.io.IOException;
import java.util.Map;

/**
 * match is indexValue & queryValue != 0
 */
public class BitCollectorFilterable extends ValueSourceCollectorFilterable {

  protected final long queryBit;

  public BitCollectorFilterable(ValueSource valueSource, long queryBit) {
    super(valueSource);
    this.queryBit = queryBit;
  }

  @Override
  public String description() {
    return "bit("+valueSource.description()+"):"+queryBit;
  }

  @Override
  public boolean matches(int doc) {
    long indexObj = functionValues.longVal(doc);
    return (indexObj & queryBit) != 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    BitCollectorFilterable that = (BitCollectorFilterable) o;

    if (queryBit != that.queryBit) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (int) (queryBit ^ (queryBit >>> 32));
    return result;
  }
}
