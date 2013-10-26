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

import org.apache.lucene.queries.function.ValueSource;

public abstract class ValueSourceCollectorFilterable extends CollectorFilterable {

  protected ValueSource valueSource;

  public ValueSourceCollectorFilterable(ValueSource valueSource) {
    this.valueSource = valueSource;
  }

  public ValueSource getValueSource() {
    return valueSource;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((valueSource == null) ? 0 : valueSource.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ValueSourceCollectorFilterable other = (ValueSourceCollectorFilterable) obj;
    if (valueSource == null) {
      if (other.valueSource != null) return false;
    } else if (!valueSource.equals(other.valueSource)) return false;
    return true;
  }

}
