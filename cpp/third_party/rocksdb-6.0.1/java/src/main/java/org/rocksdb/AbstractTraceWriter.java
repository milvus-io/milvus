// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Base class for TraceWriters.
 */
public abstract class AbstractTraceWriter
    extends RocksCallbackObject implements TraceWriter {

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewTraceWriter();
  }

  /**
   * Called from JNI, proxy for {@link TraceWriter#write(Slice)}.
   *
   * @param sliceHandle the native handle of the slice (which we do not own)
   *
   * @return short (2 bytes) where the first byte is the
   *     {@link Status.Code#getValue()} and the second byte is the
   *     {@link Status.SubCode#getValue()}.
   */
  private short writeProxy(final long sliceHandle) {
    try {
      write(new Slice(sliceHandle));
      return statusToShort(Status.Code.Ok, Status.SubCode.None);
    } catch (final RocksDBException e) {
      return statusToShort(e.getStatus());
    }
  }

  /**
   * Called from JNI, proxy for {@link TraceWriter#closeWriter()}.
   *
   * @return short (2 bytes) where the first byte is the
   *     {@link Status.Code#getValue()} and the second byte is the
   *     {@link Status.SubCode#getValue()}.
   */
  private short closeWriterProxy() {
    try {
      closeWriter();
      return statusToShort(Status.Code.Ok, Status.SubCode.None);
    } catch (final RocksDBException e) {
      return statusToShort(e.getStatus());
    }
  }

  private static short statusToShort(/*@Nullable*/ final Status status) {
    final Status.Code code = status != null && status.getCode() != null
        ? status.getCode()
        : Status.Code.IOError;
    final Status.SubCode subCode = status != null && status.getSubCode() != null
        ? status.getSubCode()
        : Status.SubCode.None;
    return statusToShort(code, subCode);
  }

  private static short statusToShort(final Status.Code code,
      final Status.SubCode subCode) {
    short result = (short)(code.getValue() << 8);
    return (short)(result | subCode.getValue());
  }

  private native long createNewTraceWriter();
}
