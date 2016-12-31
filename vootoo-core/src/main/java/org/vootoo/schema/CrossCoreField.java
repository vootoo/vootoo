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

package org.vootoo.schema;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaAware;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.uninverting.UninvertingReader;
import org.apache.solr.util.RefCounted;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * cross core fetch field value source for function sort and transform
 */
public class CrossCoreField extends FieldType implements SchemaAware {

  private String CROSS_CORE_PREFIX = "_solr_";

  private IndexSchema schema;

  @Override
  public UninvertingReader.Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    throw new UnsupportedOperationException();
  }

  private String[] dumpCrossSchemaField(SchemaField field) {
    String name = field.getName();
    assert name.startsWith(CROSS_CORE_PREFIX);

    String subName = name.substring(CROSS_CORE_PREFIX.length());

    List<String> names = Lists.newArrayList(Splitter.on('.').omitEmptyStrings().trimResults().split(subName));
    if (names.size() < 2) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          CrossCoreField.class.getSimpleName() + "'s field [" + field.getName() + "] must content '.' to split collection and target field");
    }
    String crossCoreName = names.get(0);
    String crossFieldName = null;
    if (names.size() > 2) {
      names.remove(0);
      crossFieldName = Joiner.on('.').join(names);
    } else {
      crossFieldName = names.get(1);
    }

    return new String[] {crossCoreName, crossFieldName};
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    if(info == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core field ["+field.getName()+"] must have SolrRequestInfo");
    }

    String[] crossCore = dumpCrossSchemaField(field);

    SchemaField uniqueKeyField = schema.getUniqueKeyField();

    final SolrCore targetCore = info.getReq().getCore().getCoreDescriptor().getCoreContainer().getCore(crossCore[0]);
    if(targetCore == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core field ["+field.getName()+"] miss target core ["+crossCore[0]+"]");
    }

    SchemaField targetField = targetCore.getLatestSchema().getFieldOrNull(crossCore[1]);
    if(targetField == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core field ["+field.getName()+"] core ["+crossCore[0]+"] miss field ["+crossCore[1]+"]");
    }

    final RefCounted<SolrIndexSearcher> targetCoreSearcher = targetCore.getSearcher();
    SolrIndexSearcher targetSolrIndexSearcher = targetCoreSearcher.get();

    CrossCoreFieldValueSource ccfvs = new CrossCoreFieldValueSource(uniqueKeyField, targetField, parser, targetSolrIndexSearcher);

    info.addCloseHook(new Closeable() {
      @Override
      public void close() throws IOException {
        targetCoreSearcher.decref();
        targetCore.close();
      }
    });

    return ccfvs;
  }

  private static class CrossCoreFieldValueSource extends ValueSource {

    final ValueSource mainKeySource;
    SchemaField uniqueKeyField;

    final SolrIndexSearcher targetSolrIndexSearcher;
    SchemaField targetField;
    ValueSource targetValueSource;

    public CrossCoreFieldValueSource(SchemaField uniqueKeyField, SchemaField targetField, QParser parser, SolrIndexSearcher targetSolrIndexSearcher) {
      this.mainKeySource = uniqueKeyField.getType().getValueSource(uniqueKeyField, parser);
      this.targetValueSource = targetField.getType().getValueSource(targetField, parser);
      this.uniqueKeyField = uniqueKeyField;
      this.targetField = targetField;
      this.targetSolrIndexSearcher = targetSolrIndexSearcher;

    }

    private Object toDefaultObject() {
      SchemaField field = targetField;
      if(field.getDefaultValue() != null) {
        return field.getType().toObject(field.createField(field.getDefaultValue(), 1.0f));
      }
      return null;
    }

    private class CrossCoreFunctionValues extends FunctionValues {

      final FunctionValues mainValues;
      final FunctionValues targetValues;
      Object defaultValue;

      public CrossCoreFunctionValues(FunctionValues mainValues, FunctionValues targetValues) {
        this.mainValues = mainValues;
        this.targetValues = targetValues;
        defaultValue = toDefaultObject();
      }

      public int targetDocId(int doc) {
        String mainKey = mainValues.strVal(doc);
        BytesRefBuilder brb = new BytesRefBuilder();
        uniqueKeyField.getType().readableToIndexed(mainKey, brb);
        try {
          //top doc id
          return targetSolrIndexSearcher.getFirstMatch(new Term(uniqueKeyField.getName(), brb.get()));
        } catch (IOException e) {
          //TODO log
        }

        return -1; //not found
      }

      @Override
      public byte byteVal(int doc) {
        int tdid = targetDocId(doc);
        if(tdid < 0) {
          return 0;
        }
        return targetValues.byteVal(tdid);
      }

      @Override
      public short shortVal(int doc) {
        int tdid = targetDocId(doc);
        if(tdid < 0) {
          return 0;
        }
        return targetValues.shortVal(tdid);
      }

      @Override
      public float floatVal(int doc) {
        int tdid = targetDocId(doc);
        if(tdid < 0) {
          return 0;
        }
        return targetValues.floatVal(tdid);
      }

      @Override
      public int intVal(int doc) {
        int tdid = targetDocId(doc);
        if(tdid < 0) {
          return 0;
        }
        return targetValues.intVal(tdid);
      }

      @Override
      public long longVal(int doc) {
        int tdid = targetDocId(doc);
        if(tdid < 0) {
          return 0;
        }
        return targetValues.longVal(tdid);
      }

      @Override
      public double doubleVal(int doc) {
        int tdid = targetDocId(doc);
        if(tdid < 0) {
          return 0;
        }
        return targetValues.doubleVal(tdid);
      }

      @Override
      public boolean boolVal(int doc) {
        int tdid = targetDocId(doc);
        if(tdid < 0) {
          return false;
        }
        return targetValues.boolVal(tdid);
      }

      @Override
      public boolean exists(int doc) {
        return targetDocId(doc) >= 0;
      }

      @Override
      public Object objectVal(int doc) {
        int tdid = targetDocId(doc);
        if(tdid < 0) {
          return defaultValue;
        }
        return targetValues.objectVal(tdid);
      }

      @Override
      public String toString(int doc) {
        return String.valueOf(objectVal(doc));
      }
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      FunctionValues mainValues = mainKeySource.getValues(context, readerContext);
      FunctionValues targetValues = targetValueSource.getValues(context, targetSolrIndexSearcher.getSlowAtomicReader().getContext());
      return new CrossCoreFunctionValues(mainValues, targetValues);
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public String description() {
      return "cross_core_field("+targetSolrIndexSearcher.getCore().getName()+", "+targetField.getName()+")";
    }
  }

  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;
    for(IndexSchema.DynamicField df : schema.getDynamicFields()) {
      if(df.getPrototype().getType() instanceof CrossCoreField) {
        if(!df.getPrototype().getName().startsWith(CROSS_CORE_PREFIX)) {
          throw new RuntimeException(CrossCoreField.class.getSimpleName()+"'s field define name need '"+ CROSS_CORE_PREFIX +"' prefix");
        }
      }
    }
  }
}
