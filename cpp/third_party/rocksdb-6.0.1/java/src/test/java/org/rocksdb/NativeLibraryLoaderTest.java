// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.Environment;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

import static org.assertj.core.api.Assertions.assertThat;

public class NativeLibraryLoaderTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void tempFolder() throws IOException {
    NativeLibraryLoader.getInstance().loadLibraryFromJarToTemp(
        temporaryFolder.getRoot().getAbsolutePath());
    final Path path = Paths.get(temporaryFolder.getRoot().getAbsolutePath(),
        Environment.getJniLibraryFileName("rocksdb"));
    assertThat(Files.exists(path)).isTrue();
    assertThat(Files.isReadable(path)).isTrue();
  }

  @Test
  public void overridesExistingLibrary() throws IOException {
    File first = NativeLibraryLoader.getInstance().loadLibraryFromJarToTemp(
        temporaryFolder.getRoot().getAbsolutePath());
    NativeLibraryLoader.getInstance().loadLibraryFromJarToTemp(
        temporaryFolder.getRoot().getAbsolutePath());
    assertThat(first.exists()).isTrue();
  }
}
