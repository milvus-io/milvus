// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.Test;
import org.rocksdb.MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;

public class MutableColumnFamilyOptionsTest {

  @Test
  public void builder() {
    final MutableColumnFamilyOptionsBuilder builder =
        MutableColumnFamilyOptions.builder();
        builder
            .setWriteBufferSize(10)
            .setInplaceUpdateNumLocks(5)
            .setDisableAutoCompactions(true)
            .setParanoidFileChecks(true);

    assertThat(builder.writeBufferSize()).isEqualTo(10);
    assertThat(builder.inplaceUpdateNumLocks()).isEqualTo(5);
    assertThat(builder.disableAutoCompactions()).isEqualTo(true);
    assertThat(builder.paranoidFileChecks()).isEqualTo(true);
  }

  @Test(expected = NoSuchElementException.class)
  public void builder_getWhenNotSet() {
    final MutableColumnFamilyOptionsBuilder builder =
        MutableColumnFamilyOptions.builder();

    builder.writeBufferSize();
  }

  @Test
  public void builder_build() {
    final MutableColumnFamilyOptions options = MutableColumnFamilyOptions
        .builder()
          .setWriteBufferSize(10)
          .setParanoidFileChecks(true)
          .build();

    assertThat(options.getKeys().length).isEqualTo(2);
    assertThat(options.getValues().length).isEqualTo(2);
    assertThat(options.getKeys()[0])
        .isEqualTo(
            MutableColumnFamilyOptions.MemtableOption.write_buffer_size.name());
    assertThat(options.getValues()[0]).isEqualTo("10");
    assertThat(options.getKeys()[1])
        .isEqualTo(
            MutableColumnFamilyOptions.MiscOption.paranoid_file_checks.name());
    assertThat(options.getValues()[1]).isEqualTo("true");
  }

  @Test
  public void mutableColumnFamilyOptions_toString() {
    final String str = MutableColumnFamilyOptions
        .builder()
        .setWriteBufferSize(10)
        .setInplaceUpdateNumLocks(5)
        .setDisableAutoCompactions(true)
        .setParanoidFileChecks(true)
        .build()
        .toString();

    assertThat(str).isEqualTo("write_buffer_size=10;inplace_update_num_locks=5;"
        + "disable_auto_compactions=true;paranoid_file_checks=true");
  }

  @Test
  public void mutableColumnFamilyOptions_parse() {
    final String str = "write_buffer_size=10;inplace_update_num_locks=5;"
        + "disable_auto_compactions=true;paranoid_file_checks=true";

    final MutableColumnFamilyOptionsBuilder builder =
        MutableColumnFamilyOptions.parse(str);

    assertThat(builder.writeBufferSize()).isEqualTo(10);
    assertThat(builder.inplaceUpdateNumLocks()).isEqualTo(5);
    assertThat(builder.disableAutoCompactions()).isEqualTo(true);
    assertThat(builder.paranoidFileChecks()).isEqualTo(true);
  }
}
