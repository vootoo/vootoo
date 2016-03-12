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

package org.vootoo;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.util.ReadOnlyCoresLocator;
import org.apache.solr.util.TestHarness;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class NamedCoresLocator extends ReadOnlyCoresLocator {

  List<TestHarness.TestCoresLocator> tcls = new ArrayList<>();

  public NamedCoresLocator(String ... coreName) {
    File dataBase = new File(System.getProperty("tempDir", System.getProperty("java.io.tmpdir")),("init-vootoo-data"));
    for(String name : coreName) {
      TestHarness.TestCoresLocator tcl = new TestHarness.TestCoresLocator(name,
          new File(dataBase, name).getAbsolutePath(),
          "solrconfig.xml","schema.xml");

      tcls.add(tcl);
    }
  }

  @Override
  public List<CoreDescriptor> discover(CoreContainer cc) {
    List<CoreDescriptor> cds = new ArrayList<>();
    for(TestHarness.TestCoresLocator tcl : tcls) {
      cds.add(tcl.discover(cc).get(0));
    }
    return cds;
  }
}
