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

import com.google.protobuf.ByteString;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.vootoo.client.netty.protocol.SolrProtocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * @author chenlb on 2015-05-21 14:15.
 */
public class ProtobufContentStream implements ContentStream {

  private final SolrProtocol.ContentStream contentStream;

  public ProtobufContentStream(SolrProtocol.ContentStream contentStream) {
    this.contentStream = contentStream;
  }

  @Override
  public String getName() {
    return contentStream.getName();
  }

  @Override
  public String getSourceInfo() {
    return contentStream.getSourceInfo();
  }

  @Override
  public String getContentType() {
    return contentStream.getContentType();
  }

  @Override
  public Long getSize() {
    return contentStream.getSize();
  }

  @Override
  public InputStream getStream() throws IOException {
    ByteString stream = contentStream.getStream();
    if(stream == null) {
      throw new IOException("ProtobufContentStream not found stream!");
    }
    return stream.newInput();
  }

  @Override
  public Reader getReader() throws IOException {
    String charset = ContentStreamBase.getCharsetFromContentType(getContentType());
    return charset == null
        ? new InputStreamReader( getStream(), ContentStreamBase.DEFAULT_CHARSET )
        : new InputStreamReader( getStream(), charset );
  }
}
