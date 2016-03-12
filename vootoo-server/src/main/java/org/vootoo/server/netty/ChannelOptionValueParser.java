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

package org.vootoo.server.netty;

import org.apache.commons.lang.StringUtils;

/**
 * channel option string parser to ChannelOption Value
 * @author chenlb on 2015-05-22 10:48.
 */
public interface ChannelOptionValueParser<V> {

  /**
   * @return value is blank or null return null
   * @throws Exception parse fail
   */
  V parse(String value) throws Exception;

  public static class LongOptionValueParser implements ChannelOptionValueParser<Long> {
    @Override
    public Long parse(String value) throws Exception {
      if(StringUtils.isBlank(value)) {
        return null;
      } else {
        return Long.parseLong(value);
      }
    }
  }
}
