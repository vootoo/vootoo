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

package org.vootoo.client.netty.util;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.vootoo.client.netty.protocol.SolrProtocol;
import org.vootoo.common.VootooException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * protobuf and solr util
 */
public class ProtobufUtil {

  public static ModifiableSolrParams toSolrParams(SolrProtocol.SolrRequest request) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    List<SolrProtocol.Param> paramsList = request.getParamList();
    if(paramsList != null) {
      for(SolrProtocol.Param param : paramsList) {
        List<String> vs = param.getValueList();
        if(vs == null) {
          params.add(param.getKey(), null);
        } else {
          params.add(param.getKey(), vs.toArray(new String[vs.size()]));
        }
      }
    }
    return params;
  }

  private static SolrProtocol.Param toProtobufParam(String key, String[] values) {
    SolrProtocol.Param.Builder builder = SolrProtocol.Param.newBuilder();
    builder.setKey(key);
    for(String v : values) {
      builder.addValue(v);
    }
    return builder.build();
  }

  public static List<SolrProtocol.Param> toProtobufParams(SolrParams solrParams) {
    List<SolrProtocol.Param> paramList;
    if(solrParams instanceof ModifiableSolrParams) {
      ModifiableSolrParams mps = (ModifiableSolrParams) solrParams;
      Map<String,String[]> maps = mps.getMap();
      paramList = new ArrayList<>(maps.size());
      for(Map.Entry<String, String[]> e : maps.entrySet()) {
        paramList.add(toProtobufParam(e.getKey(), e.getValue()));
      }
    } else {
      paramList = new ArrayList<>();
      Iterator<String> it = solrParams.getParameterNamesIterator();
      while(it.hasNext()) {
        String key = it.next();
        String[] values = solrParams.getParams(key);
        paramList.add(toProtobufParam(key, values));
      }
    }
    return paramList;
  }

  public static Collection<ContentStream> toSolrContentStreams(SolrProtocol.SolrRequest request) {
    return Collections2.transform(request.getContentStreamList(), new Function<SolrProtocol.ContentStream,ContentStream>() {
      @Override
      public ContentStream apply(SolrProtocol.ContentStream input) {
        return new ProtobufContentStream(input);
      }
    });
  }

  public static List<SolrProtocol.ContentStream> toProtobufContentStreams(Collection<ContentStream> contentStreams) {
    return Lists.transform(Lists.newArrayList(contentStreams), new Function<ContentStream, SolrProtocol.ContentStream>() {
      @Override
      public SolrProtocol.ContentStream apply(ContentStream input) {
        SolrProtocol.ContentStream.Builder stream = SolrProtocol.ContentStream.newBuilder();

        if(input.getName() != null) {
          stream.setName(input.getName());
        }
        String contentType = input.getContentType();
        if(contentType == null) {
          // default javabin
          contentType = BinaryResponseParser.BINARY_CONTENT_TYPE;
        }
        stream.setContentType(contentType);
        stream.setSourceInfo(input.getSourceInfo());
        stream.setSize(input.getSize());
        try {
          //TODO zero copy byte
          stream.setStream(ByteString.readFrom(input.getStream()));
        } catch (IOException e) {
          //TODO dealing input.getStream() exception
          throw new RuntimeException("ContentStream.getStream() exception", e);
        }

        return stream.build();
      }
    });
  }

  /**
   * @return not response body return null.
   */
  public static InputStream getSolrResponseInputStream(SolrProtocol.SolrResponse protocolResponse) {
    if(protocolResponse.hasResponseBody()) {
      return protocolResponse.getResponseBody().getBody().newInput();
    } else {
      return null;
    }
  }

  public static String getResponseBodyCharset(SolrProtocol.SolrResponse protocolResponse) {
    String charset = ContentStreamBase.DEFAULT_CHARSET;
    if(protocolResponse != null) {
      SolrProtocol.ResponseBody responseBody = protocolResponse.getResponseBody();
      if(responseBody != null) {
        charset = ContentStreamBase.getCharsetFromContentType(responseBody.getContentType());
      }
    }
    return charset;
  }

  protected static NamedList<String> toSolrExceptionMetadata(List<SolrProtocol.KeyValue> keyValues) {
    NamedList<String> metadata = new NamedList<>();
    for(SolrProtocol.KeyValue kv : keyValues) {
      metadata.add(kv.getKey(), kv.getValue());
    }
    return metadata;
  }

  public static void fillErrorMetadata(SolrProtocol.ExceptionBody.Builder protocolSolrException, NamedList<String> errorMetadata) {
    if (errorMetadata != null) {
      //errorMetadata one by one
      Iterator<Map.Entry<String,String>> it = errorMetadata.iterator();
      while(it.hasNext()) {
        Map.Entry<String,String> next = it.next();
        SolrProtocol.KeyValue.Builder metaBuilder = SolrProtocol.KeyValue.newBuilder();
        metaBuilder.setKey(next.getKey());
        metaBuilder.setValue(next.getValue());

        // errorMetadata one
        protocolSolrException.addMetadata(metaBuilder);
      }
    }
  }

  public static int getErrorInfo(Throwable ex, SolrProtocol.ExceptionBody.Builder exceptionBody) {
    int code = 500;

    if(ex instanceof SolrException) {
      SolrException solrExc = (SolrException)ex;
      code = solrExc.code();
      fillErrorMetadata(exceptionBody, solrExc.getMetadata());
    }

    // add first has msg
    for (Throwable th = ex; th != null; th = th.getCause()) {
      String msg = th.getMessage();
      if (msg != null) {
        exceptionBody.setMessage(msg);
        break;
      }
    }

    //trace
    if (code == 500 || code < 100) {
      exceptionBody.setTrace(SolrException.toStr(ex));
      // non standard codes have undefined results with various servers
      if (code < 100) {
        code = 500;
      }
    }

    exceptionBody.setCode(code);

    return code;
  }

  /**
   * @return SolrException or VootooException
   */
  public static VootooException toVootooException(SolrProtocol.ExceptionBody exceptionBody) {
    String msg = exceptionBody.getMessage();
    VootooException.VootooErrorCode vec = VootooException.VootooErrorCode.getErrorCode(exceptionBody.getCode());
    VootooException ve = new VootooException(vec, msg);
    if(vec == VootooException.VootooErrorCode.UNKNOWN) {
      ve.setUnknownCode(exceptionBody.getCode());
    }

    if(exceptionBody.getMetadataCount() > 0) {
      NamedList<String> metadata = toSolrExceptionMetadata(exceptionBody.getMetadataList());
      ve.setMetadata(metadata);
    }

    if(exceptionBody.hasTrace()) {
      ve.setRemoteTrace(exceptionBody.getTrace());
    }

    return ve;
  }
}
