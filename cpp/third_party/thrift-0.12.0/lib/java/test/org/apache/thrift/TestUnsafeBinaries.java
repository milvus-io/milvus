/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift;

import java.nio.ByteBuffer;
import java.util.Arrays;

import thrift.test.SafeBytes;
import thrift.test.UnsafeBytes;

//  test generating types with un-copied byte[]/ByteBuffer input/output
//
public class TestUnsafeBinaries extends TestStruct {

  private static byte[] input() {
    return new byte[]{1, 1};
  }

  //
  //  verify that the unsafe_binaries option modifies behavior
  //

  //  constructor doesn't copy
  public void testUnsafeConstructor() throws Exception {

    byte[] input = input();
    UnsafeBytes struct = new UnsafeBytes(ByteBuffer.wrap(input));

    input[0] = 2;

    assertTrue(Arrays.equals(
        new byte[]{2, 1},
        struct.getBytes())
    );

  }

  //  getter doesn't copy
  //  note: this behavior is the same with/without the flag, but if this default ever changes, the current behavior
  //        should be retained when using this flag
  public void testUnsafeGetter(){
    UnsafeBytes struct = new UnsafeBytes(ByteBuffer.wrap(input()));

    byte[] val = struct.getBytes();
    val[0] = 2;

    assertTrue(Arrays.equals(
        new byte[]{2, 1},
        struct.getBytes())
    );

  }

  //  setter doesn't copy
  public void testUnsafeSetter(){
    UnsafeBytes struct = new UnsafeBytes();

    byte[] val = input();
    struct.setBytes(val);

    val[0] = 2;

    assertTrue(Arrays.equals(
        new byte[]{2, 1},
        struct.getBytes())
    );

  }

  //  buffer doens't copy
  public void testUnsafeBufferFor(){
    UnsafeBytes struct = new UnsafeBytes(ByteBuffer.wrap(input()));

    ByteBuffer val = struct.bufferForBytes();
    val.array()[0] = 2;

    assertTrue(Arrays.equals(
        new byte[]{2, 1},
        struct.getBytes())
    );

  }

  //
  //  verify that the default generator does not change behavior
  //

  public void testSafeConstructor() {

    byte[] input = input();
    SafeBytes struct = new SafeBytes(ByteBuffer.wrap(input));

    input[0] = 2;

    assertTrue(Arrays.equals(
        new byte[]{1, 1},
        struct.getBytes())
    );

  }

  public void testSafeSetter() {

    byte[] input = input();
    SafeBytes struct = new SafeBytes(ByteBuffer.wrap(input));

    input[0] = 2;

    assertTrue(Arrays.equals(
        new byte[]{1, 1},
        struct.getBytes())
    );

  }

  public void testSafeBufferFor(){
    SafeBytes struct = new SafeBytes(ByteBuffer.wrap(input()));

    ByteBuffer val = struct.bufferForBytes();
    val.array()[0] = 2;

    assertTrue(Arrays.equals(
        new byte[]{1, 1},
        struct.getBytes())
    );

  }

}
