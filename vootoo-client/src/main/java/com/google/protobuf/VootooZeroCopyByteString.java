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

package com.google.protobuf;

/**
 * copy com.google.protobuf.HBaseZeroCopyByteString
 * @author chenlb on 2015-05-25 21:28.
 */
public class VootooZeroCopyByteString extends LiteralByteString {

  /** Private constructor so this class cannot be instantiated. */
  private VootooZeroCopyByteString() {
    super(null);
    throw new UnsupportedOperationException("Should never be here.");
  }

  public static ByteString wrap(byte[] array) {
    return new LiteralByteString(array);
  }

  public static ByteString wrap(byte[] array, int offset, int length) {
    return new BoundedByteString(array, offset, length);
  }

  /**
   * Extracts the byte array from the given {@link ByteString} without copy.
   * @param buf A buffer from which to extract the array.  This buffer must be
   * actually an instance of a {@link LiteralByteString}.
   */
  public static byte[] zeroCopyGetBytes(final ByteString buf) {
    if (buf instanceof LiteralByteString) {
      return ((LiteralByteString) buf).bytes;
    }
    throw new UnsupportedOperationException("Need a LiteralByteString, got a " + buf.getClass().getName());
  }
}
