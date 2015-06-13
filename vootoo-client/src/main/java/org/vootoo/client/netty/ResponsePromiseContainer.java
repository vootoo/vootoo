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

import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chenlb on 2015-06-13 13:54.
 */
public class ResponsePromiseContainer {

  /** Global_Container */
  public static final ResponsePromiseContainer GLOBAL_CONTAINER = new ResponsePromiseContainer();

  protected AtomicLong ridSeed = new AtomicLong(0);
  protected ConcurrentMap<Long, ResponsePromise> responsePromiseMaps = PlatformDependent.newConcurrentHashMap();

  public ResponsePromise createResponsePromise() {
    long rid = ridSeed.incrementAndGet();
    ResponsePromise responsePromise = new ResponsePromise(rid);
    do {
      ResponsePromise oldPromise = responsePromiseMaps.putIfAbsent(rid, responsePromise);
      if (oldPromise != null) {
        rid = ridSeed.incrementAndGet();
      } else {
        responsePromise.setRid(rid);
        break;
      }
    } while (true);
    return responsePromise;
  }

  public ResponsePromise removeResponsePromise(long rid) {
    return responsePromiseMaps.remove(rid);
  }
}
