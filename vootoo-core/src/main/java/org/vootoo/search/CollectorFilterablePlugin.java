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

import java.util.*;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.vootoo.search.function.filter.BitCollectorFilterablePlugin;
import org.vootoo.search.function.filter.BitContainCollectorFilterablePlugin;
import org.vootoo.search.function.filter.InCollectorFilterablePlugin;
import org.vootoo.search.function.filter.RangeCollectorFilterablePlugin;

/**
 * {@link CollectorFilterable} build plugin
 */
public abstract class CollectorFilterablePlugin {

  public static final Map<String,CollectorFilterablePlugin> standardPlugins = new HashMap<String,CollectorFilterablePlugin>();

  static {
    standardPlugins.put("in", new InCollectorFilterablePlugin());
    standardPlugins.put("range", new RangeCollectorFilterablePlugin());
    standardPlugins.put("bit", new BitCollectorFilterablePlugin());
    standardPlugins.put("cbit", new BitContainCollectorFilterablePlugin());
  }

  /** return a {@link CollectorFilterable} */
  public abstract CollectorFilterable createCollectorFilterable(String qstr,
      SolrParams localParams, SolrParams params, SolrQueryRequest req,
      ValueSource valueSource,
      String valueStr)
          throws SyntaxError;

  public abstract String getName();

  public void verifyValueStr(String valueStr, ValueSource valueSource) throws SyntaxError {
    if (valueStr == null) {
      throw new SyntaxError("cf=" + getName() + " '" + valueSource.description()
          + "' value is null");
    }
    valueStr = valueStr.trim();
    if (valueStr.length() < 1) {
      throw new SyntaxError("cf=" + getName() + " '" + valueSource.description()
          + "' value is blank");
    }
  }

  /**
   * @param valueStr
   *          format is :<i>x or (x[<, n>...])</i>
   * @param splitStr
   *          null is ','
   * @param requireValue is true value is not blank
   * @return list value
   * @throws SyntaxError format error
   */
  public static List<String> parseMultiValue(String valueStr, String splitStr,
      boolean requireValue)
          throws SyntaxError {

    if (splitStr == null) {
      splitStr = ",";
    }

    valueStr = valueStr.trim();

    List<String> lvs = new ArrayList<String>();

    if ('(' == valueStr.charAt(0)) {
      if (')' == valueStr.charAt(valueStr.length() - 1)) {
        String vstr = valueStr.substring(1, valueStr.length() - 1);
        String[] vs = vstr.split(splitStr);
        if (vs == null || vs.length < 1) {
          throw new SyntaxError("value format is 'x or (x[<, n>...])'! q="
              + valueStr);
        }
        for (String s : vs) {
          if (s != null) {
            s = s.trim();
            if (s.length() > 0) {
              lvs.add(s);
            }
          }
        }
      } else {
        throw new SyntaxError(
            "value format is 'x or (x[<, n>...])', miss ')' q=" + valueStr);
      }
    } else {
      // single
      lvs.add(valueStr);
    }

    if (requireValue && lvs.size() < 1) {
      throw new SyntaxError(
          "value format is 'x or (x[<, n>...])', not values! q=" + valueStr);
    }

    return lvs;
  }

  /**
   * @param rangeValue
   *          [|(v1 TO v2)|]
   *
   * @throws SyntaxError if format error
   */
  public static ValueSourceRangeFilter parseRange(ValueSource vs,
      String rangeValue) throws SyntaxError {
    boolean includeLower = true;
    boolean includeUpper = true;

    if(rangeValue == null) {
      throw new SyntaxError("range query format error, rangeValue is null");
    }

    rangeValue = rangeValue.trim();

    // "[ TO ]".length
    if (rangeValue.length() < 6) {
      throw new SyntaxError("range query format error, rangeValue="
          + rangeValue);
    }
    char ch = rangeValue.charAt(0);
    if (ch == '(') {
      includeLower = false;
    } else {
      if (ch != '[') {
        throw new SyntaxError(
            "range query lower char must be '(' or '[', rangeValue="
                + rangeValue);
      }
    }

    ch = rangeValue.charAt(rangeValue.length() - 1);
    if (ch == ')') {
      includeUpper = false;
    } else {
      if (ch != ']') {
        throw new SyntaxError(
            "range query upper char must be ')' or ']', rangeValue="
                + rangeValue);
      }
    }

    String l = null;
    String u = null;

    String[] values = rangeValue.substring(1, rangeValue.length() - 1)
        .split("\\ TO\\ ");
    if (values == null || values.length < 2) {
      throw new SyntaxError("range query format error, rangeValue="
          + rangeValue);
    }
    values[0] = values[0].trim();
    values[1] = values[1].trim();
    if (values[0].length() > 0 && !values[0].equals("*")) {
      l = values[0];
    }
    if (values[1].length() > 0 && !values[1].equals("*")) {
      u = values[1];
    }

    return new ValueSourceRangeFilter(vs, l, u, includeLower, includeUpper);
  }

  private static Map<String, Boolean> radix_16;
  static {
    Map<String, Boolean> map = new HashMap<>(4);
    // 16 radix
    map.put("0x", Boolean.TRUE);
    map.put("0X", Boolean.TRUE);

    // 2 radix
    map.put("0b", Boolean.FALSE);
    map.put("0B", Boolean.FALSE);

    radix_16 = Collections.unmodifiableMap(map);
  }

  /**
   * Long.parseLong and can parse 0x or 0b radix number
   * @throws NumberFormatException
   */
  public static Long parseLongExt(String longStr) {
    try {
      return Long.parseLong(longStr);
    } catch (NumberFormatException e) {
      if(longStr.length() > 2) {
        //try parse 0x or 0b
        String lstr = longStr.substring(0, 2);
        Boolean isRadix16 = radix_16.get(lstr);
        if(isRadix16 == null) {
          throw e;
        } else {
          if(isRadix16) {
            return Long.parseLong(longStr.substring(2), 16);
          } else {
            return Long.parseLong(longStr.substring(2), 2);
          }
        }
      } else {
        throw e;
      }
    }
  }
}
