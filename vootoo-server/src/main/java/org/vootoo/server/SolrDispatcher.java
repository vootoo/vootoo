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

package org.vootoo.server;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.ResponseUtils;
import org.apache.solr.servlet.SolrRequestParsers;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.util.FastWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.client.ResponseFormat;
import org.vootoo.client.netty.SolrRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;

/**
*/
public class SolrDispatcher implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(SolrDispatcher.class);

  public static final Charset UTF8 = Charset.forName("UTF-8");

  private final SolrRequest solrRequest;
  private final CoreContainer cores;

  private SolrCore core;
  private SolrRequestHandler handler;
  private SolrQueryRequest solrReq;
  private SolrQueryResponse solrRsp;
  private QueryResponseWriter responseWriter;

  private Throwable throwable;

  public SolrDispatcher (SolrRequest solrRequest, CoreContainer coreContainer) {
    this.solrRequest = solrRequest;
    this.cores = coreContainer;
  }

  @Override
  public void run () {

    try {
      dispatch();
    } catch (Throwable t) {
      throwable = t;
      logger.error("", t);
    } finally {
      try {
        if (solrReq != null) {
          logger.debug("Closing out SolrRequest: {}", solrReq);
          solrReq.close();
        }
      } finally {
        try {
          if (core != null) {
            core.close();
          }
        } finally {
          SolrRequestInfo.clearRequestInfo();
        }
      }
    }

  }

  private void dispatch() throws Exception {
    String path = solrRequest.getPath();

    // Check for the core admin page
    if(path.equals(cores.getAdminPath())) {
      handler = cores.getMultiCoreHandler();
      handleAdminRequest();
      return;
    }

    // Check for the core admin collections url
    if(path.equals("/admin/collections")) {
      handler = cores.getCollectionsHandler();
      handleAdminRequest();
      return;
    }

    // Check for the core admin info url
    if( path.startsWith( "/admin/info" ) ) {
      handler = cores.getInfoHandler();
      handleAdminRequest();
      return;
    }

    String coreName = solrRequest.getCoreName();
    core = cores.getCore(coreName);

    // With a valid core...
    if(core != null) {
      final SolrConfig config = core.getSolrConfig();
      // get or create/cache the parser for the core
      SolrRequestParsers parser = config.getRequestParsers();

      // Handle /schema/* paths via Restlet
      if( path.startsWith("/schema") ) {
        //solrReq = parseSolrQueryRequest(core, path);
        //SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, new SolrQueryResponse()));
        throw new UnsupportedOperationException("/schema* unsupported");
      }

      // Determine the handler from the url path if not set
      // (we might already have selected the cores handler)
      if( handler == null && path.length() > 1 ) { // don't match "" or "/" as valid path
        handler = core.getRequestHandler( path );
        // no handler yet but allowed to handle select; let's check
        if( handler == null && parser.isHandleSelect() ) {
          if( "/select".equals( path ) || "/select/".equals( path ) ) {
            solrReq = parseSolrQueryRequest(core, parser);
            String qt = solrReq.getParams().get( CommonParams.QT );
            handler = core.getRequestHandler( qt );
            if( handler == null ) {
              throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unknown handler: "+qt);
            }
            if( qt != null && qt.startsWith("/") && (handler instanceof ContentStreamHandlerBase)) {
              //For security reasons it's a bad idea to allow a leading '/', ex: /select?qt=/update see SOLR-3161
              //There was no restriction from Solr 1.4 thru 3.5 and it's not supported for update handlers.
              throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Invalid Request Handler ('qt').  Do not use /select to access: "+qt);
            }
          }
        }
      }

      // With a valid handler and a valid core...
      if( handler != null ) {
        // if not a /select, create the request
        if( solrReq == null ) {
          solrReq = parseSolrQueryRequest(core, parser);
        }

        //TODO
        /*
        if (usingAliases) {
          processAliases(solrReq, aliases, collectionsList);
        }
        */

        solrRsp = new SolrQueryResponse();
        SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, solrRsp));

        // execute
        execute();

        return; // we are done with a valid handler
      } else {
        //TODO  check handler == null
      }
    } else {
      //TODO  check core == null

    }

  }

  private void execute() {
    solrReq.getContext().put("path", solrRequest.getPath());
    solrReq.getContext().put("webapp", "netty-server");
    core.execute(handler, solrReq, solrRsp);
    responseWriter = core.getQueryResponseWriter(solrReq);
  }

  private SolrQueryRequest parseSolrQueryRequest (SolrCore core, SolrRequestParsers parsers) throws Exception {
    ArrayList<ContentStream> streams = new ArrayList<ContentStream>(1);
    SolrQueryRequest request = parsers.buildRequestFrom(core, solrRequest.getSolrParams(), streams);
    return request;
  }

  private void handleAdminRequest() throws Exception {
    solrReq = parseSolrQueryRequest(null, SolrRequestParsers.DEFAULT);
    solrRsp = new SolrQueryResponse();

    SolrCore.preDecorateResponse(solrReq, solrRsp);
    handler.handleRequest(solrReq, solrRsp);
    SolrCore.postDecorateResponse(handler, solrReq, solrRsp);
    if(logger.isInfoEnabled() && solrRsp.getToLog().size() > 0) {
      logger.info(solrRsp.getToLogAsString("[admin] "));
    }
    QueryResponseWriter respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get(solrReq.getParams().get(CommonParams.WT));
    if(respWriter == null) {
      respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get("standard");
    }
    responseWriter = respWriter;
  }

  public int getResponseFormat() {
    return ResponseFormat.JAVA_BIN.getFormat();
  }

  public void writeResponse(OutputStream outputStream) throws IOException {
    assert responseWriter != null;
    writeResponse(solrRsp, outputStream, responseWriter, solrReq, Method.POST);
  }

  private static void writeResponse(SolrQueryResponse solrRsp, OutputStream outputStream,
      QueryResponseWriter responseWriter, SolrQueryRequest solrReq, Method reqMethod)
      throws IOException {

    // Now write it out
    final String ct = responseWriter.getContentType(solrReq, solrRsp);
    //TODO write content type to response


    if (Method.HEAD != reqMethod) {
      if (responseWriter instanceof BinaryQueryResponseWriter) {
        BinaryQueryResponseWriter binWriter = (BinaryQueryResponseWriter) responseWriter;
        binWriter.write(outputStream, solrReq, solrRsp);
      } else {
        String charset = ContentStreamBase.getCharsetFromContentType(ct);
        Writer out = (charset == null || charset.equalsIgnoreCase("UTF-8"))
            ? new OutputStreamWriter(outputStream, UTF8)
            : new OutputStreamWriter(outputStream, charset);
        out = new FastWriter(out);
        responseWriter.write(out, solrReq, solrRsp);
        out.flush();
      }
    }
    //else http HEAD request, nothing to write out, waited this long just to get ContentType
  }

  public Throwable getThrowable () {
    return throwable;
  }

  public void sendError(OutputStream outputStream,
      ErrorCodeSetter errorCodeSetter,
      Throwable ex) throws IOException {

    SolrQueryRequest req = solrReq;
    final SolrParams solrParams = solrRequest.getSolrParams();
    Exception exp = null;
    SolrCore localCore = core;
    try {
      SolrQueryResponse solrResp = new SolrQueryResponse();
      if(ex instanceof Exception) {
        solrResp.setException((Exception)ex);
      }
      else {
        solrResp.setException(new RuntimeException(ex));
      }
      if(core==null) {
        localCore = cores.getCore(""); // default core
      } else {
        localCore = core;
      }
      if(req==null) {
        req = new SolrQueryRequestBase(core, solrParams) {};
      }
      QueryResponseWriter writer = core.getQueryResponseWriter(req);
      writeResponse(solrResp, outputStream, writer, req, Method.POST);
    }
    catch (Exception e) { // This error really does not matter
      exp = e;
    } finally {
      try {
        if (exp != null) {
          SimpleOrderedMap info = new SimpleOrderedMap();
          int code = ResponseUtils.getErrorInfo(ex, info, logger);
          errorCodeSetter.setError(code, info.toString());
        }
      } finally {
        if (core == null && localCore != null) {
          localCore.close();
        }
      }
    }
  }
}
