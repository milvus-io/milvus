// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import org.rocksdb.Status.Code;
import org.rocksdb.Status.SubCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class RocksDBExceptionTest {

  @Test
  public void exception() {
    try {
      raiseException();
    } catch(final RocksDBException e) {
      assertThat(e.getStatus()).isNull();
      assertThat(e.getMessage()).isEqualTo("test message");
      return;
    }
    fail();
  }

  @Test
  public void exceptionWithStatusCode() {
    try {
      raiseExceptionWithStatusCode();
    } catch(final RocksDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.NotSupported);
      assertThat(e.getStatus().getSubCode()).isEqualTo(SubCode.None);
      assertThat(e.getStatus().getState()).isNull();
      assertThat(e.getMessage()).isEqualTo("test message");
      return;
    }
    fail();
  }

  @Test
  public void exceptionNoMsgWithStatusCode() {
    try {
      raiseExceptionNoMsgWithStatusCode();
    } catch(final RocksDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.NotSupported);
      assertThat(e.getStatus().getSubCode()).isEqualTo(SubCode.None);
      assertThat(e.getStatus().getState()).isNull();
      assertThat(e.getMessage()).isEqualTo(Code.NotSupported.name());
      return;
    }
    fail();
  }

  @Test
  public void exceptionWithStatusCodeSubCode() {
    try {
      raiseExceptionWithStatusCodeSubCode();
    } catch(final RocksDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.TimedOut);
      assertThat(e.getStatus().getSubCode())
          .isEqualTo(Status.SubCode.LockTimeout);
      assertThat(e.getStatus().getState()).isNull();
      assertThat(e.getMessage()).isEqualTo("test message");
      return;
    }
    fail();
  }

  @Test
  public void exceptionNoMsgWithStatusCodeSubCode() {
    try {
      raiseExceptionNoMsgWithStatusCodeSubCode();
    } catch(final RocksDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.TimedOut);
      assertThat(e.getStatus().getSubCode()).isEqualTo(SubCode.LockTimeout);
      assertThat(e.getStatus().getState()).isNull();
      assertThat(e.getMessage()).isEqualTo(Code.TimedOut.name() +
          "(" + SubCode.LockTimeout.name() + ")");
      return;
    }
    fail();
  }

  @Test
  public void exceptionWithStatusCodeState() {
    try {
      raiseExceptionWithStatusCodeState();
    } catch(final RocksDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.NotSupported);
      assertThat(e.getStatus().getSubCode()).isEqualTo(SubCode.None);
      assertThat(e.getStatus().getState()).isNotNull();
      assertThat(e.getMessage()).isEqualTo("test message");
      return;
    }
    fail();
  }

  private native void raiseException() throws RocksDBException;
  private native void raiseExceptionWithStatusCode() throws RocksDBException;
  private native void raiseExceptionNoMsgWithStatusCode() throws RocksDBException;
  private native void raiseExceptionWithStatusCodeSubCode()
      throws RocksDBException;
  private native void raiseExceptionNoMsgWithStatusCodeSubCode()
      throws RocksDBException;
  private native void raiseExceptionWithStatusCodeState()
      throws RocksDBException;
}
