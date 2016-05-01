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

package org.vootoo.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.util.DateMathParser;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * use {@link FilterCollector} call {@link #matches(int)} determine docId if hit or not.
 */
public abstract class CollectorFilterable {

	public abstract boolean equals(Object o);

	public abstract int hashCode();

	public abstract String description();

	public abstract boolean matches(int doc);

	public abstract void doSetNextReader(@SuppressWarnings("rawtypes") Map context, LeafReaderContext readerContext) throws IOException;

	public static interface Parser {
		Object parse(String value);
	}

	public static class ByteParser implements Parser {

		@Override
		public Object parse(String value) {
			return Byte.parseByte(value);
		}

	}

	public static class ShortParser implements Parser {

		@Override
		public Object parse(String value) {
			return Short.parseShort(value);
		}

	}

	public static class IntParser implements Parser {

		@Override
		public Object parse(String value) {
			return Integer.parseInt(value);
		}

	}

	public static class LongParser implements Parser {

		@Override
		public Object parse(String value) {
			return Long.parseLong(value);
		}

	}

	public static class FloatParser implements Parser {

		@Override
		public Object parse(String value) {
			return Float.parseFloat(value);
		}

	}

	public static class DoubleParser implements Parser {

		@Override
		public Object parse(String value) {
			return Double.parseDouble(value);
		}

	}

	public static class StringParser implements Parser {

		@Override
		public Object parse(String value) {
			return value;
		}

	}

	public static class CharParser implements Parser {

		@Override
		public Object parse(String value) {
			return Character.valueOf(value.charAt(0));
		}

	}

	public static class BoolParser implements Parser {

		@Override
		public Object parse(String value) {
			return Boolean.parseBoolean(value);
		}

	}

	public static class DateParser implements Parser {

		@Override
		public Object parse(String value) {
			return DateMathParser.parseMath(null, value);
		}

	}

	public static List<Object> parseValue(String[] values, Parser parser) {
		List<Object> vs = new ArrayList<Object>(values.length);
		for(String value : values) {
			vs.add(parser.parse(value));
		}
		return vs;
	}

	public static List<Object> parseValue(String[] values, Object targetObject) {
		if(targetObject.getClass().isPrimitive()) {
			Class<?> clazz = targetObject.getClass();
			if(Byte.TYPE.isAssignableFrom(clazz)) {
				return parseValue(values, new ByteParser());
			} else if(Short.TYPE.isAssignableFrom(clazz)) {
				return parseValue(values, new ShortParser());
			} else if(Integer.TYPE.isAssignableFrom(clazz)) {
				return parseValue(values, new IntParser());
			} else if(Long.TYPE.isAssignableFrom(clazz)) {
				return parseValue(values, new LongParser());
			} else if(Double.TYPE.isAssignableFrom(clazz)) {
				return parseValue(values, new DoubleParser());
			} else if(Float.TYPE.isAssignableFrom(clazz)) {
				return parseValue(values, new FloatParser());
			} else if(Boolean.TYPE.isAssignableFrom(clazz)) {
				return parseValue(values, new BoolParser());
			} else if(Character.TYPE.isAssignableFrom(clazz)) {
				return parseValue(values, new CharParser());
			} else {
				return parseValue(values, new StringParser());
			}
		} else if(targetObject instanceof Number) {
			if(targetObject instanceof Integer) {
				return parseValue(values, new IntParser());
			} else if(targetObject instanceof Double) {
				return parseValue(values, new DoubleParser());
			} else if(targetObject instanceof Long) {
				return parseValue(values, new LongParser());
			} else if(targetObject instanceof Byte) {
				return parseValue(values, new ByteParser());
			} else if(targetObject instanceof Short) {
				return parseValue(values, new ShortParser());
			} else if(targetObject instanceof Float) {
				return parseValue(values, new FloatParser());
			} else {
				return parseValue(values, new DoubleParser());
			}
		} else if(targetObject instanceof String) {
			return parseValue(values, new StringParser());
		} else if(targetObject instanceof Boolean) {
			return parseValue(values, new BoolParser());
		} else if(targetObject instanceof Date) {
			return parseValue(values, new DateParser());
		} else if(targetObject instanceof Character) {
			return parseValue(values, new CharParser());
		} else { //unknow type
			return parseValue(values, new StringParser());
		}
	}

}
