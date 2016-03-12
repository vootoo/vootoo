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
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;

import java.util.*;

/**
 * @author chenlb on 2015-05-24 15:35.
 */
public class Vootoo {

  public static final String UPDATE_PATH = "/update";
  public static final String UPDATE_PREFIX = "/update/";

  public static final String MDC_NAME_RID = "rid";
  public static final String MDC_NAME_REQUEST_SIZE = "request_size";
  public static final String MDC_NAME_REQUEST_PATH = "request_path";
  public static final String MDC_NAME_EXECUTE_TIME = "execute_time";
  public static final String MDC_NAME_WAIT_TIME = "wait_time";
  public static final String MDC_NAME_REMOTE_ADDRESS = "remote_address";//remoteAddress

  public static boolean isUpdateRequest(String path) {
    if(StringUtils.isBlank(path)) {
      return false;
    }
    return UPDATE_PATH.equals(path) || path.startsWith(UPDATE_PREFIX);
  }

  public static String requestLogName(String suffix) {
    String mySuffix;
    if(suffix == null) {
      mySuffix = "request";
    } else {
      mySuffix = "request."+suffix;
    }
    return Vootoo.class.getPackage().getName()+"."+mySuffix;
  }

  public static Map<String , Integer> checkStateIsValid(CoreContainer cores, String stateVer) {
    Map<String, Integer> result = null;
    String[] pairs = null;
    if (stateVer != null && !stateVer.isEmpty() && cores.isZooKeeperAware()) {
      // many have multiple collections separated by |
      pairs = StringUtils.split(stateVer, '|');
      for (String pair : pairs) {
        String[] pcs = StringUtils.split(pair, ':');
        if (pcs.length == 2 && !pcs[0].isEmpty() && !pcs[1].isEmpty()) {
          Integer status = cores.getZkController().getZkStateReader().compareStateVersions(pcs[0], Integer.parseInt(pcs[1]));
          if(status != null ){
            if(result == null) result =  new HashMap<>();
            result.put(pcs[0], status);
          }
        }
      }
    }
    return result;
  }

  public static void processAliases(SolrQueryRequest solrReq, Aliases aliases, List<String> collectionsList) {
    String collection = solrReq.getParams().get("collection");
    if (collection != null) {
      collectionsList = StrUtils.splitSmart(collection, ",", true);
    }
    if (collectionsList != null) {
      Set<String> newCollectionsList = new HashSet<>(collectionsList.size());
      for (String col : collectionsList) {
        String al = aliases.getCollectionAlias(col);
        if (al != null) {
          List<String> aliasList = StrUtils.splitSmart(al, ",", true);
          newCollectionsList.addAll(aliasList);
        } else {
          newCollectionsList.add(col);
        }
      }
      if (newCollectionsList.size() > 0) {
        StringBuilder collectionString = new StringBuilder();
        Iterator<String> it = newCollectionsList.iterator();
        int sz = newCollectionsList.size();
        for (int i = 0; i < sz; i++) {
          collectionString.append(it.next());
          if (i < newCollectionsList.size() - 1) {
            collectionString.append(",");
          }
        }
        ModifiableSolrParams params = new ModifiableSolrParams(solrReq.getParams());
        params.set("collection", collectionString.toString());
        solrReq.setParams(params);
      }
    }
  }

  public static SolrCore getCoreByCollection(CoreContainer cores, String corename) {
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();

    ClusterState clusterState = zkStateReader.getClusterState();
    Map<String,Slice> slices = clusterState.getActiveSlicesMap(corename);
    if (slices == null) {
      return null;
    }
    // look for a core on this node
    Set<Map.Entry<String,Slice>> entries = slices.entrySet();
    SolrCore core = null;
    done:
    for (Map.Entry<String,Slice> entry : entries) {
      // first see if we have the leader
      ZkNodeProps leaderProps = clusterState.getLeader(corename, entry.getKey());
      if (leaderProps != null) {
        core = checkProps(cores, leaderProps);
      }
      if (core != null) {
        break done;
      }

      // check everyone then
      Map<String,Replica> shards = entry.getValue().getReplicasMap();
      Set<Map.Entry<String,Replica>> shardEntries = shards.entrySet();
      for (Map.Entry<String,Replica> shardEntry : shardEntries) {
        Replica zkProps = shardEntry.getValue();
        core = checkProps(cores, zkProps);
        if (core != null) {
          break done;
        }
      }
    }
    return core;
  }

  public static SolrCore checkProps(CoreContainer cores, ZkNodeProps zkProps) {
    String corename;
    SolrCore core = null;
    if (cores.getZkController().getNodeName().equals(zkProps.getStr(ZkStateReader.NODE_NAME_PROP))) {
      corename = zkProps.getStr(ZkStateReader.CORE_NAME_PROP);
      core = cores.getCore(corename);
    }
    return core;
  }


}
