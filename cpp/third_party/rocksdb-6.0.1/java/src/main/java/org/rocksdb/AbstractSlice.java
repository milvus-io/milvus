// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Slices are used by RocksDB to provide
 * efficient access to keys and values.
 *
 * This class is package private, implementers
 * should extend either of the public abstract classes:
 *   @see org.rocksdb.Slice
 *   @see org.rocksdb.DirectSlice
 *
 * Regards the lifecycle of Java Slices in RocksDB:
 *   At present when you configure a Comparator from Java, it creates an
 *   instance of a C++ BaseComparatorJniCallback subclass and
 *   passes that to RocksDB as the comparator. That subclass of
 *   BaseComparatorJniCallback creates the Java
 *   @see org.rocksdb.AbstractSlice subclass Objects. When you dispose
 *   the Java @see org.rocksdb.AbstractComparator subclass, it disposes the
 *   C++ BaseComparatorJniCallback subclass, which in turn destroys the
 *   Java @see org.rocksdb.AbstractSlice subclass Objects.
 */
public abstract class AbstractSlice<T> extends RocksMutableObject {

  protected AbstractSlice() {
    super();
  }

  protected AbstractSlice(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Returns the data of the slice.
   *
   * @return The slice data. Note, the type of access is
   *   determined by the subclass
   *   @see org.rocksdb.AbstractSlice#data0(long)
   */
  public T data() {
    return data0(getNativeHandle());
  }

  /**
   * Access to the data is provided by the
   * subtype as it needs to handle the
   * generic typing.
   *
   * @param handle The address of the underlying
   *   native object.
   *
   * @return Java typed access to the data.
   */
  protected abstract T data0(long handle);

  /**
   * Drops the specified {@code n}
   * number of bytes from the start
   * of the backing slice
   *
   * @param n The number of bytes to drop
   */
  public abstract void removePrefix(final int n);

  /**
   * Clears the backing slice
   */
  public abstract void clear();

  /**
   * Return the length (in bytes) of the data.
   *
   * @return The length in bytes.
   */
  public int size() {
    return size0(getNativeHandle());
  }

  /**
   * Return true if the length of the
   * data is zero.
   *
   * @return true if there is no data, false otherwise.
   */
  public boolean empty() {
    return empty0(getNativeHandle());
  }

  /**
   * Creates a string representation of the data
   *
   * @param hex When true, the representation
   *   will be encoded in hexadecimal.
   *
   * @return The string representation of the data.
   */
  public String toString(final boolean hex) {
    return toString0(getNativeHandle(), hex);
  }

  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Three-way key comparison
   *
   *  @param other A slice to compare against
   *
   *  @return Should return either:
   *    1) &lt; 0 if this &lt; other
   *    2) == 0 if this == other
   *    3) &gt; 0 if this &gt; other
   */
  public int compare(final AbstractSlice<?> other) {
    assert (other != null);
    if(!isOwningHandle()) {
      return other.isOwningHandle() ? -1 : 0;
    } else {
      if(!other.isOwningHandle()) {
        return 1;
      } else {
        return compare0(getNativeHandle(), other.getNativeHandle());
      }
    }
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  /**
   * If other is a slice object, then
   * we defer to {@link #compare(AbstractSlice) compare}
   * to check equality, otherwise we return false.
   *
   * @param other Object to test for equality
   *
   * @return true when {@code this.compare(other) == 0},
   *   false otherwise.
   */
  @Override
  public boolean equals(final Object other) {
    if (other != null && other instanceof AbstractSlice) {
      return compare((AbstractSlice<?>)other) == 0;
    } else {
      return false;
    }
  }

  /**
   * Determines whether this slice starts with
   * another slice
   *
   * @param prefix Another slice which may of may not
   *   be a prefix of this slice.
   *
   * @return true when this slice starts with the
   *   {@code prefix} slice
   */
  public boolean startsWith(final AbstractSlice<?> prefix) {
    if (prefix != null) {
      return startsWith0(getNativeHandle(), prefix.getNativeHandle());
    } else {
      return false;
    }
  }

  protected native static long createNewSliceFromString(final String str);
  private native int size0(long handle);
  private native boolean empty0(long handle);
  private native String toString0(long handle, boolean hex);
  private native int compare0(long handle, long otherHandle);
  private native boolean startsWith0(long handle, long otherHandle);

  /**
   * Deletes underlying C++ slice pointer.
   * Note that this function should be called only after all
   * RocksDB instances referencing the slice are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected final native void disposeInternal(final long handle);

}
