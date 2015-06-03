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

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.RequestHandlers;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.logging.MDCUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.QueryResponseWriterUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.ResponseUtils;
import org.apache.solr.servlet.SolrRequestParsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vootoo.RequestGetter;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.vootoo.server.Vootoo.*;

/**
 * process solr request, copy and Modify from {@link org.apache.solr.servlet.SolrDispatchFilter#doFilter(ServletRequest, ServletResponse, FilterChain)}
 *
 * @author chenlb on 2015-05-25 11:14.
 */
public class RequestProcesser {

  private static final Logger logger = LoggerFactory.getLogger(RequestProcesser.class);

  private final CoreContainer cores;
  private final ResponseSetter responseSetter;

  //===== create from handle request
  private SolrCore core = null;
  private SolrQueryRequest solrReq = null;
  private Aliases aliases = null;

  //The states of client that is invalid in this request
  private Map<String, Integer> invalidStates = null;

  SolrParams solrParams;

  public RequestProcesser(CoreContainer cores, ResponseSetter responseSetter) {
    this.cores = cores;
    this.responseSetter = responseSetter;
  }

  public void handleRequest(RequestGetter requestGetter) {
    MDCUtils.clearMDC();

    String path = requestGetter.getPath();
    solrParams = requestGetter.getSolrParams();
    SolrRequestHandler handler = null;
    String corename = "";
    String origCorename = null;
    try {
      // set a request timer which can be reused by requests if needed
      //req.setAttribute(SolrRequestParsers.REQUEST_TIMER_SERVLET_ATTRIBUTE, new RTimer());
      // put the core container in request attribute
      //req.setAttribute("org.apache.solr.CoreContainer", cores);
      // check for management path
      String alternate = cores.getManagementPath();
      if (alternate != null && path.startsWith(alternate)) {
        path = path.substring(0, alternate.length());
      }
      // unused feature ?
      int idx = path.indexOf( ':' );
      if( idx > 0 ) {
        // save the portion after the ':' for a 'handler' path parameter
        path = path.substring( 0, idx );
      }


      boolean usingAliases = false;
      List<String> collectionsList = null;

      // Check for container handlers
      handler = cores.getRequestHandler(path);
      if (handler != null) {
        solrReq = parseSolrQueryRequest(SolrRequestParsers.DEFAULT, requestGetter);
        handleAdminRequest(handler, solrReq);
        return;
      }
      else {
        //otherwise, we should find a core from the path
        idx = path.indexOf( "/", 1 );
        if( idx > 1 ) {
          // try to get the corename as a request parameter first
          corename = path.substring( 1, idx );

          // look at aliases
          if (cores.isZooKeeperAware()) {
            origCorename = corename;
            ZkStateReader reader = cores.getZkController().getZkStateReader();
            aliases = reader.getAliases();
            if (aliases != null && aliases.collectionAliasSize() > 0) {
              usingAliases = true;
              String alias = aliases.getCollectionAlias(corename);
              if (alias != null) {
                collectionsList = StrUtils.splitSmart(alias, ",", true);
                corename = collectionsList.get(0);
              }
            }
          }

          core = cores.getCore(corename);

          if (core != null) {
            path = path.substring( idx );
            addMDCValues(cores, core);
          }
        }

        //add collection name
        if(core == null && StringUtils.isNotBlank(requestGetter.getCollection())) {
          corename = requestGetter.getCollection();
          core = cores.getCore(corename);
          if(core != null) {
            addMDCValues(cores, core);
          }
        }

        if (core == null) {
          if (!cores.isZooKeeperAware() ) {
            core = cores.getCore("");
            if (core != null) {
              addMDCValues(cores, core);
            }
          }
        }
      }

      if (core == null && cores.isZooKeeperAware()) {
        // we couldn't find the core - lets make sure a collection was not specified instead
        core = getCoreByCollection(cores, corename);

        if (core != null) {
          // we found a core, update the path
          path = path.substring( idx );
          addMDCValues(cores, core);
        }

        // try the default core
        if (core == null) {
          core = cores.getCore("");
          if (core != null) {
            addMDCValues(cores, core);
          }
        }
      }

      // With a valid core...
      if( core != null ) {
        final SolrConfig config = core.getSolrConfig();
        // get or create/cache the parser for the core
        SolrRequestParsers parser = config.getRequestParsers();


        // Determine the handler from the url path if not set
        // (we might already have selected the cores handler)
        if( handler == null && path.length() > 1 ) { // don't match "" or "/" as valid path
          handler = core.getRequestHandler( path );

          if(handler == null){
            //may be a restlet path
            // Handle /schema/* paths via Restlet
            if( path.equals("/schema") || path.startsWith("/schema/")) {
              throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unsupport /schema/**, use http solr");
            }

          }
          // no handler yet but allowed to handle select; let's check
          if( handler == null && parser.isHandleSelect() ) {
            if( "/select".equals( path ) || "/select/".equals( path ) ) {
              solrReq = parseSolrQueryRequest(parser, requestGetter);

              invalidStates = checkStateIsValid(cores,solrReq.getParams().get(CloudSolrClient.STATE_VERSION));
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
            solrReq = parseSolrQueryRequest(parser, requestGetter);
          }

          if (usingAliases) {
            processAliases(solrReq, aliases, collectionsList);
          }

          SolrQueryResponse solrRsp = new SolrQueryResponse();
          SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, solrRsp));
          this.execute(handler, solrReq, solrRsp);
          QueryResponseWriter responseWriter = core.getQueryResponseWriter(solrReq);
          if(invalidStates != null) solrReq.getContext().put(CloudSolrClient.STATE_VERSION, invalidStates);
          writeResponse(solrRsp, responseWriter, solrReq);

          return; // we are done with a valid handler
        }
      }
      logger.debug("no handler or core retrieved for {}, follow through...", path);
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "no handler or core retrieved for "+path);
    }
    catch (Throwable ex) {
      sendError(core, solrReq, ex);
      // walk the the entire cause chain to search for an Error
      Throwable t = ex;
      while (t != null) {
        if (t instanceof Error)  {
          if (t != ex)  {
            logger.error("An Error was wrapped in another exception - please report complete stacktrace on SOLR-6161", ex);
          }
          throw (Error) t;
        }
        t = t.getCause();
      }
      return;
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

  protected void execute(SolrRequestHandler handler, SolrQueryRequest sreq, SolrQueryResponse rsp) {
    // used for logging query stats in SolrCore.execute()
    sreq.getContext().put( "webapp", "vootoo" );
    sreq.getCore().execute( handler, sreq, rsp );
  }

  protected void writeResponse(SolrQueryResponse solrRsp, QueryResponseWriter responseWriter, SolrQueryRequest solrReq) throws IOException {
    Object invalidStates = solrReq.getContext().get(CloudSolrClient.STATE_VERSION);
    //This is the last item added to the applyResult and the client would expect it that way.
    //If that assumption is changed , it would fail. This is done to avoid an O(n) scan on
    // the applyResult for each request
    if(invalidStates != null) solrRsp.add(CloudSolrClient.STATE_VERSION, invalidStates);
    // Now write it out
    final String ct = responseWriter.getContentType(solrReq, solrRsp);
    // don't call setContentType on null
    if (null != ct) {
      responseSetter.setContentType(ct);
    }

    if (solrRsp.getException() != null) {
      NamedList info = new SimpleOrderedMap();
      int code = ResponseUtils.getErrorInfo(solrRsp.getException(), info, logger);
      //solrRsp.add("error", info);
      // use protocol response exception instead of set 'error' response return to client,
      responseSetter.setSolrResponseException(code, info);
    }

    QueryResponseWriterUtil.writeQueryResponse(responseSetter.getResponseOutputStream(), responseWriter, solrReq, solrRsp, ct);

    //fire QueryResponse write Complete
    responseSetter.writeQueryResponseComplete(solrRsp);
  }

  protected void sendError(SolrCore core, SolrQueryRequest req, Throwable ex) {
    responseSetter.addError(ex);
  }


  protected SolrQueryRequest parseSolrQueryRequest(SolrRequestParsers parser, RequestGetter requestGetter) throws Exception {
    ArrayList<ContentStream> streams = new ArrayList<>(1);
    if( requestGetter.getContentStreams() != null && requestGetter.getContentStreams().size() > 0 ) {
      streams.addAll(requestGetter.getContentStreams());
    }
    SolrQueryRequest sreq = parser.buildRequestFrom(core, requestGetter.getSolrParams(), streams);

    // Handlers and login will want to know the path. If it contains a ':'
    // the handler could use it for RESTful URLs
    sreq.getContext().put( "path", RequestHandlers.normalize(requestGetter.getPath()) );

    return sreq;
  }

  protected void handleAdminRequest(SolrRequestHandler handler, SolrQueryRequest solrReq) throws IOException {
    SolrQueryResponse solrResp = new SolrQueryResponse();
    SolrCore.preDecorateResponse(solrReq, solrResp);
    handler.handleRequest(solrReq, solrResp);
    SolrCore.postDecorateResponse(handler, solrReq, solrResp);
    if (logger.isInfoEnabled() && solrResp.getToLog().size() > 0) {
      logger.info(solrResp.getToLogAsString("[admin] "));
    }
    QueryResponseWriter respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get(solrReq.getParams().get(CommonParams.WT));
    if (respWriter == null) respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get("standard");
    writeResponse(solrResp, respWriter, solrReq);
  }
}
