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

package org.vootoo.client;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * query only solr client. when invoke update method throw {@link UnsupportedOperationException}
 * @author chenlb on 2015-06-02 10:45.
 */
public abstract class QueryOnlySolrClient extends SolrClient {

  @Override
  public final UpdateResponse add(String collection, Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse add(String collection, Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse add(String collection, SolrInputDocument doc) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse add(String collection, SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse add(String collection, Iterator<SolrInputDocument> docIterator) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse add(Iterator<SolrInputDocument> docIterator) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported add operation!");
  }

  @Override
  public final UpdateResponse addBean(String collection, Object obj) throws IOException, SolrServerException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBean operation!");
  }

  @Override
  public final UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBean operation!");
  }

  @Override
  public final UpdateResponse addBean(String collection, Object obj, int commitWithinMs) throws IOException, SolrServerException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBean operation!");
  }

  @Override
  public final UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException, SolrServerException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBean operation!");
  }

  @Override
  public final UpdateResponse addBeans(String collection, Collection<?> beans) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBeans operation!");
  }

  @Override
  public final UpdateResponse addBeans(Collection<?> beans) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBeans operation!");
  }

  @Override
  public final UpdateResponse addBeans(String collection, Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBeans operation!");
  }

  @Override
  public final UpdateResponse addBeans(Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBeans operation!");
  }

  @Override
  public final UpdateResponse addBeans(String collection, Iterator<?> beanIterator) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBeans operation!");
  }

  @Override
  public final UpdateResponse addBeans(Iterator<?> beanIterator) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported addBeans operation!");
  }

  @Override
  public final UpdateResponse commit(String collection) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported commit operation!");
  }

  @Override
  public final UpdateResponse commit() throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported commit operation!");
  }

  @Override
  public final UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported commit operation!");
  }

  @Override
  public final UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported commit operation!");
  }

  @Override
  public final UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher, boolean softCommit) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported commit operation!");
  }

  @Override
  public final UpdateResponse commit(boolean waitFlush, boolean waitSearcher, boolean softCommit) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported commit operation!");
  }

  @Override
  public final UpdateResponse optimize(String collection) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported optimize operation!");
  }

  @Override
  public final UpdateResponse optimize() throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported optimize operation!");
  }

  @Override
  public final UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported optimize operation!");
  }

  @Override
  public final UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported optimize operation!");
  }

  @Override
  public final UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher, int maxSegments) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported optimize operation!");
  }

  @Override
  public final UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported optimize operation!");
  }

  @Override
  public final UpdateResponse rollback(String collection) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported rollback operation!");
  }

  @Override
  public final UpdateResponse rollback() throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported rollback operation!");
  }

  @Override
  public final UpdateResponse deleteById(String collection, String id) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteById operation!");
  }

  @Override
  public final UpdateResponse deleteById(String id) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteById operation!");
  }

  @Override
  public final UpdateResponse deleteById(String collection, String id, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteById operation!");
  }

  @Override
  public final UpdateResponse deleteById(String id, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteById operation!");
  }

  @Override
  public final UpdateResponse deleteById(String collection, List<String> ids) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteById operation!");
  }

  @Override
  public final UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteById operation!");
  }

  @Override
  public final UpdateResponse deleteById(String collection, List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteById operation!");
  }

  @Override
  public final UpdateResponse deleteById(List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteById operation!");
  }

  @Override
  public final UpdateResponse deleteByQuery(String collection, String query) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteByQuery operation!");
  }

  @Override
  public final UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteByQuery operation!");
  }

  @Override
  public final UpdateResponse deleteByQuery(String collection, String query, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteByQuery operation!");
  }

  @Override
  public final UpdateResponse deleteByQuery(String query, int commitWithinMs) throws SolrServerException, IOException {
    throw new UnsupportedOperationException(QueryOnlySolrClient.class.getSimpleName()+" Unsupported deleteByQuery operation!");
  }
}
