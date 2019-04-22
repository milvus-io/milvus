// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb.util;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SizeUnitTest {

  public static final long COMPUTATION_UNIT = 1024L;

  @Test
  public void sizeUnit() {
    assertThat(SizeUnit.KB).isEqualTo(COMPUTATION_UNIT);
    assertThat(SizeUnit.MB).isEqualTo(
        SizeUnit.KB * COMPUTATION_UNIT);
    assertThat(SizeUnit.GB).isEqualTo(
        SizeUnit.MB * COMPUTATION_UNIT);
    assertThat(SizeUnit.TB).isEqualTo(
        SizeUnit.GB * COMPUTATION_UNIT);
    assertThat(SizeUnit.PB).isEqualTo(
        SizeUnit.TB * COMPUTATION_UNIT);
  }
}
