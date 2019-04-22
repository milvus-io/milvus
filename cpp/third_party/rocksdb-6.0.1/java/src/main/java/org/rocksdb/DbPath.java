// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.file.Path;

/**
 * Tuple of database path and target size
 */
public class DbPath {
  final Path path;
  final long targetSize;

  public DbPath(final Path path, final long targetSize) {
    this.path = path;
    this.targetSize = targetSize;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DbPath dbPath = (DbPath) o;

    if (targetSize != dbPath.targetSize) {
      return false;
    }

    return path != null ? path.equals(dbPath.path) : dbPath.path == null;
  }

  @Override
  public int hashCode() {
    int result = path != null ? path.hashCode() : 0;
    result = 31 * result + (int) (targetSize ^ (targetSize >>> 32));
    return result;
  }
}
