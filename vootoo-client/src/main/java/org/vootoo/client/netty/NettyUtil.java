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

package org.vootoo.client.netty;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Iterator;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;

import com.google.protobuf.ByteString;

/**
 */
public class NettyUtil {

  /**
   * @param bs
   * @return bs == null，返回 null
   * @throws IOException
   */
  public static NamedList<Object> bytesToParams(ByteString bs) throws IOException {
    if (bs == null) {
      return null;
    }
    return (NamedList<Object>) new JavaBinCodec().unmarshal(bs.newInput());
  }

  public static SolrParams fromString(String params) {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    String[] kvs = params.split("&");
    for (String kv : kvs) {
      int idx = kv.indexOf("=");
      if (idx > 0 && idx < kv.length() - 1) {
        try {
          solrParams.add(kv.substring(0, idx), URLDecoder.decode(kv.substring(idx + 1, kv.length()), "utf-8"));
        } catch (UnsupportedEncodingException e) {
          // cat
        }
      }
    }

    return solrParams;
  }

  public static ByteString formSolrParams(SolrParams solrParams) {
    return ByteString.copyFromUtf8(solrParamsToString(solrParams, "&"));
  }

  public static String solrParamsToString(SolrParams solrParams, String joinStr) {
    StringBuilder sb = new StringBuilder(128);
    Iterator<String> it = solrParams.getParameterNamesIterator();
    while(it.hasNext()) {
      String name = it.next();
      String[] values = solrParams.getParams(name);
      if(values == null) {
        //only key
        sb.append(name);
        sb.append(joinStr);
      } else {
        for(String v : values) {
          sb.append(name);
          sb.append('=');
          sb.append(v);
          sb.append(joinStr);
        }
      }
    }
    if(sb.length() > joinStr.length()) {
      sb.setLength(sb.length() - joinStr.length());
    }
    return sb.toString();
  }

  public static ByteString readFrom(ContentStream stream) throws IOException {
    InputStream in = stream.getStream();
    return ByteString.readFrom(in);
  }

  public static ContentStreamBase readFrom(ByteString bytes) {
    return new ContentStreamBase.ByteArrayStream(bytes.toByteArray(), "ByteString_ContentStream");
  }
}
