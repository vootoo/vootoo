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
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

/**
 * <ul>
 *   <li>rand() - Random.nextInt(Integer.MAX_VALUE)</li>
 *   <li>rand(max_range)</li>
 *   <li>rand(max_range, seed)</li>
 * </ul>
 * <br/>
 * solrconfig.xml add
 * <br/>
 * &lt;valueSourceParser name="rand" class="org.vootoo.search.function.RandomValueSourceParser" /&gt;
 *
 * @author chenlb on 2016-12-24 09:34.
 */
public class RandomValueSourceParser extends ValueSourceParser {

  protected class RandomValueSource extends ValueSource {
    private int max = Integer.MAX_VALUE;
    private Long seed = null;
    private ThreadLocal<Random> random = null;

    public RandomValueSource() {
      this(Integer.MAX_VALUE);
    }

    public RandomValueSource(int max) {
      this(max, null);
    }

    public RandomValueSource(int max, final Long seed) {
      this.max = max;
      this.seed = seed;
      random = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
          return seed == null ? new Random() : new Random(seed);
        }
      };
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      return new IntDocValues(this) {
        @Override
        public int intVal(int doc) {
          return random.get().nextInt(max);
        }
      };
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RandomValueSource that = (RandomValueSource) o;

      if (max != that.max) return false;
      return seed != null ? seed.equals(that.seed) : that.seed == null;

    }

    @Override
    public int hashCode() {
      int result = seed != null ? seed.hashCode() : 0;
      result = 31 * result + max;
      return result;
    }

    @Override
    public String description() {
      return seed == null ? "rand("+max+")" : "rand("+max+","+seed+")";
    }
  }

  protected long parseLong(FunctionQParser fp) throws SyntaxError {
    String str = fp.parseArg();
    if (fp.argWasQuoted()) throw new SyntaxError("Expected double instead of quoted string:" + str);
    long value = Long.parseLong(str);
    return value;
  }

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    int max = Integer.MAX_VALUE;
    Long seed = null;
    if(fp.hasMoreArguments()) {
      max = fp.parseInt();
    }
    if(fp.hasMoreArguments()) {
      seed = parseLong(fp);
    }
    return new RandomValueSource(max, seed);
  }
}
