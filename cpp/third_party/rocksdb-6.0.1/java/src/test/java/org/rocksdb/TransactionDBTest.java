// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TransactionDBTest {

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void open() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
                 dbFolder.getRoot().getAbsolutePath())) {
      assertThat(tdb).isNotNull();
    }
  }

  @Test
  public void open_columnFamilies() throws RocksDBException {
    try(final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions myCfOpts = new ColumnFamilyOptions()) {

      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("myCf".getBytes(), myCfOpts));

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
           final TransactionDB tdb = TransactionDB.open(dbOptions, txnDbOptions,
               dbFolder.getRoot().getAbsolutePath(),
               columnFamilyDescriptors, columnFamilyHandles)) {
        try {
          assertThat(tdb).isNotNull();
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void beginTransaction() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath());
        final WriteOptions writeOptions = new WriteOptions()) {

      try(final Transaction txn = tdb.beginTransaction(writeOptions)) {
        assertThat(txn).isNotNull();
      }
    }
  }

  @Test
  public void beginTransaction_transactionOptions() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions();
         final TransactionOptions txnOptions = new TransactionOptions()) {

      try(final Transaction txn = tdb.beginTransaction(writeOptions,
          txnOptions)) {
        assertThat(txn).isNotNull();
      }
    }
  }

  @Test
  public void beginTransaction_withOld() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions()) {

      try(final Transaction txn = tdb.beginTransaction(writeOptions)) {
        final Transaction txnReused = tdb.beginTransaction(writeOptions, txn);
        assertThat(txnReused).isSameAs(txn);
      }
    }
  }

  @Test
  public void beginTransaction_withOld_transactionOptions()
      throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions();
         final TransactionOptions txnOptions = new TransactionOptions()) {

      try(final Transaction txn = tdb.beginTransaction(writeOptions)) {
        final Transaction txnReused = tdb.beginTransaction(writeOptions,
            txnOptions, txn);
        assertThat(txnReused).isSameAs(txn);
      }
    }
  }

  @Test
  public void lockStatusData() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions();
         final ReadOptions readOptions = new ReadOptions()) {

      try (final Transaction txn = tdb.beginTransaction(writeOptions)) {

        final byte key[] = "key".getBytes(UTF_8);
        final byte value[] = "value".getBytes(UTF_8);

        txn.put(key, value);
        assertThat(txn.getForUpdate(readOptions, key, true)).isEqualTo(value);

        final Map<Long, TransactionDB.KeyLockInfo> lockStatus =
            tdb.getLockStatusData();

        assertThat(lockStatus.size()).isEqualTo(1);
        final Set<Map.Entry<Long, TransactionDB.KeyLockInfo>> entrySet = lockStatus.entrySet();
        final Map.Entry<Long, TransactionDB.KeyLockInfo> entry = entrySet.iterator().next();
        final long columnFamilyId = entry.getKey();
        assertThat(columnFamilyId).isEqualTo(0);
        final TransactionDB.KeyLockInfo keyLockInfo = entry.getValue();
        assertThat(keyLockInfo.getKey()).isEqualTo(new String(key, UTF_8));
        assertThat(keyLockInfo.getTransactionIDs().length).isEqualTo(1);
        assertThat(keyLockInfo.getTransactionIDs()[0]).isEqualTo(txn.getId());
        assertThat(keyLockInfo.isExclusive()).isTrue();
      }
    }
  }

  @Test
  public void deadlockInfoBuffer() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath())) {

      // TODO(AR) can we cause a deadlock so that we can test the output here?
      assertThat(tdb.getDeadlockInfoBuffer()).isEmpty();
    }
  }

  @Test
  public void setDeadlockInfoBufferSize() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath())) {
      tdb.setDeadlockInfoBufferSize(123);
    }
  }
}
