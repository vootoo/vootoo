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

package org.vootoo.common;

import org.apache.solr.common.SolrException;

/**
 * @author chenlb on 2015-06-01 16:33.
 */
public class VootooException extends SolrException {

  public enum VootooErrorCode {
    BAD_REQUEST( 400 ),
    UNAUTHORIZED( 401 ),
    FORBIDDEN( 403 ),
    NOT_FOUND( 404 ),
    CONFLICT( 409 ),
    UNSUPPORTED_MEDIA_TYPE( 415 ),
    SERVER_ERROR( 500 ),
    SERVICE_UNAVAILABLE( 503 ),
    INVALID_STATE( 510 ),

    //====vootoo===
    /** timeout */
    TIMEOUT(408),
    /** Too Many Requests */
    TOO_MANY_REQUESTS(429),
    UNKNOWN(0);
    public final int code;

    VootooErrorCode( int c ) {
      code = c;
    }

    public static VootooErrorCode getErrorCode(int c) {
      for (VootooErrorCode err : values()) {
        if(err.code == c) return err;
      }
      return UNKNOWN;
    }
  }

  private String remoteServer;
  private String remoteTrace;
  private int unknownCode;

  public VootooException(ErrorCode errorCode, String msg) {
    super(errorCode.code, msg, null);
  }

  public VootooException(VootooErrorCode errorCode, String msg) {
    super(errorCode.code, msg, null);
  }

  public VootooException(ErrorCode errorCode, String msg, Throwable th) {
    super(errorCode.code, msg, th);
  }

  public VootooException(VootooErrorCode errorCode, Throwable th) {
    super(errorCode.code, null, th);
  }

  public VootooException(VootooErrorCode errorCode, String msg, Throwable th) {
    super(errorCode.code, msg, th);
  }

  public int getUnknownCode() {
    return unknownCode;
  }

  public void setUnknownCode(int unknownCode) {
    this.unknownCode = unknownCode;
  }

  public String getRemoteTrace() {
    return remoteTrace;
  }

  public void setRemoteTrace(String remoteTrace) {
    this.remoteTrace = remoteTrace;
  }

  public String getRemoteServer() {
    return remoteServer;
  }

  public void setRemoteServer(String remoteServer) {
    this.remoteServer = remoteServer;
  }
}
