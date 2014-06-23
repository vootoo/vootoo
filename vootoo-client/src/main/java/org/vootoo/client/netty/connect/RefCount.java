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

package org.vootoo.client.netty.connect;

import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public abstract class RefCount<Type> {
  protected final Type resource;
  protected final AtomicInteger refcount = new AtomicInteger();

  public RefCount(Type resource) {
    this.resource = resource;
  }

  public int getRefcount() {
    return refcount.get();
  }

  public final RefCount<Type> incref() {
    refcount.incrementAndGet();
    return this;
  }

  public final Type get() {
    return resource;
  }

  public void decref() {
    if (refcount.decrementAndGet() == 0) {
      close();
    }
  }

  protected abstract void close();

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((resource == null) ? 0 : resource.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    @SuppressWarnings("rawtypes")
    RefCount other = (RefCount) obj;
    if (resource == null) {
      if (other.resource != null)
        return false;
    } else if (!resource.equals(other.resource))
      return false;
    return true;
  }
}
