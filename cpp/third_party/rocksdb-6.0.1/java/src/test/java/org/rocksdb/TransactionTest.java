// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TransactionTest extends AbstractTransactionTest {

  @Test
  public void getForUpdate_cf_conflict() throws RocksDBException {
    final byte k1[] = "key1".getBytes(UTF_8);
    final byte v1[] = "value1".getBytes(UTF_8);
    final byte v12[] = "value12".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();

      try(final Transaction txn = dbContainer.beginTransaction()) {
        txn.put(testCf, k1, v1);
        assertThat(txn.getForUpdate(readOptions, testCf, k1, true)).isEqualTo(v1);
        txn.commit();
      }

      try(final Transaction txn2 = dbContainer.beginTransaction()) {
        try(final Transaction txn3 = dbContainer.beginTransaction()) {
          assertThat(txn3.getForUpdate(readOptions, testCf, k1, true)).isEqualTo(v1);

          // NOTE: txn2 updates k1, during txn3
          try {
            txn2.put(testCf, k1, v12); // should cause an exception!
          } catch(final RocksDBException e) {
            assertThat(e.getStatus().getCode()).isSameAs(Status.Code.TimedOut);
            return;
          }
        }
      }

      fail("Expected an exception for put after getForUpdate from conflicting" +
          "transactions");
    }
  }

  @Test
  public void getForUpdate_conflict() throws RocksDBException {
    final byte k1[] = "key1".getBytes(UTF_8);
    final byte v1[] = "value1".getBytes(UTF_8);
    final byte v12[] = "value12".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions()) {

      try(final Transaction txn = dbContainer.beginTransaction()) {
        txn.put(k1, v1);
        assertThat(txn.getForUpdate(readOptions, k1, true)).isEqualTo(v1);
        txn.commit();
      }

      try(final Transaction txn2 = dbContainer.beginTransaction()) {
        try(final Transaction txn3 = dbContainer.beginTransaction()) {
          assertThat(txn3.getForUpdate(readOptions, k1, true)).isEqualTo(v1);

          // NOTE: txn2 updates k1, during txn3
          try {
            txn2.put(k1, v12); // should cause an exception!
          } catch(final RocksDBException e) {
            assertThat(e.getStatus().getCode()).isSameAs(Status.Code.TimedOut);
            return;
          }
        }
      }

      fail("Expected an exception for put after getForUpdate from conflicting" +
          "transactions");
    }
  }

  @Test
  public void multiGetForUpdate_cf_conflict() throws RocksDBException {
    final byte keys[][] = new byte[][] {
        "key1".getBytes(UTF_8),
        "key2".getBytes(UTF_8)};
    final byte values[][] = new byte[][] {
        "value1".getBytes(UTF_8),
        "value2".getBytes(UTF_8)};
    final byte[] otherValue = "otherValue".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();
      final List<ColumnFamilyHandle> cfList = Arrays.asList(testCf, testCf);

      try(final Transaction txn = dbContainer.beginTransaction()) {
        txn.put(testCf, keys[0], values[0]);
        txn.put(testCf, keys[1], values[1]);
        assertThat(txn.multiGet(readOptions, cfList, keys)).isEqualTo(values);
        txn.commit();
      }

      try(final Transaction txn2 = dbContainer.beginTransaction()) {
        try(final Transaction txn3 = dbContainer.beginTransaction()) {
          assertThat(txn3.multiGetForUpdate(readOptions, cfList, keys))
              .isEqualTo(values);

          // NOTE: txn2 updates k1, during txn3
          try {
            txn2.put(testCf, keys[0], otherValue); // should cause an exception!
          } catch(final RocksDBException e) {
            assertThat(e.getStatus().getCode()).isSameAs(Status.Code.TimedOut);
            return;
          }
        }
      }

      fail("Expected an exception for put after getForUpdate from conflicting" +
          "transactions");
    }
  }

  @Test
  public void multiGetForUpdate_conflict() throws RocksDBException {
    final byte keys[][] = new byte[][] {
        "key1".getBytes(UTF_8),
        "key2".getBytes(UTF_8)};
    final byte values[][] = new byte[][] {
        "value1".getBytes(UTF_8),
        "value2".getBytes(UTF_8)};
    final byte[] otherValue = "otherValue".getBytes(UTF_8);

    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions()) {
      try(final Transaction txn = dbContainer.beginTransaction()) {
        txn.put(keys[0], values[0]);
        txn.put(keys[1], values[1]);
        assertThat(txn.multiGet(readOptions, keys)).isEqualTo(values);
        txn.commit();
      }

      try(final Transaction txn2 = dbContainer.beginTransaction()) {
        try(final Transaction txn3 = dbContainer.beginTransaction()) {
          assertThat(txn3.multiGetForUpdate(readOptions, keys))
              .isEqualTo(values);

          // NOTE: txn2 updates k1, during txn3
          try {
            txn2.put(keys[0], otherValue); // should cause an exception!
          } catch(final RocksDBException e) {
            assertThat(e.getStatus().getCode()).isSameAs(Status.Code.TimedOut);
            return;
          }
        }
      }

      fail("Expected an exception for put after getForUpdate from conflicting" +
          "transactions");
    }
  }

  @Test
  public void name() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.getName()).isEmpty();
      final String name = "my-transaction-" + rand.nextLong();
      txn.setName(name);
      assertThat(txn.getName()).isEqualTo(name);
    }
  }

  @Test
  public void ID() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.getID()).isGreaterThan(0);
    }
  }

  @Test
  public void deadlockDetect() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.isDeadlockDetect()).isFalse();
    }
  }

  @Test
  public void waitingTxns() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.getWaitingTxns().getTransactionIds().length).isEqualTo(0);
    }
  }

  @Test
  public void state() throws RocksDBException {
    try(final DBContainer dbContainer = startDb()) {

      try(final Transaction txn = dbContainer.beginTransaction()) {
        assertThat(txn.getState())
            .isSameAs(Transaction.TransactionState.STARTED);
        txn.commit();
        assertThat(txn.getState())
            .isSameAs(Transaction.TransactionState.COMMITED);
      }

      try(final Transaction txn = dbContainer.beginTransaction()) {
        assertThat(txn.getState())
            .isSameAs(Transaction.TransactionState.STARTED);
        txn.rollback();
        assertThat(txn.getState())
            .isSameAs(Transaction.TransactionState.STARTED);
      }
    }
  }

  @Test
  public void Id() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.getId()).isNotNull();
    }
  }

  @Override
  public TransactionDBContainer startDb() throws RocksDBException {
    final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
    final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor(TXN_TEST_COLUMN_FAMILY,
                columnFamilyOptions));
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    final TransactionDB txnDb;
    try {
      txnDb = TransactionDB.open(options, txnDbOptions,
          dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors,
              columnFamilyHandles);
    } catch(final RocksDBException e) {
      columnFamilyOptions.close();
      txnDbOptions.close();
      options.close();
      throw e;
    }

    final WriteOptions writeOptions = new WriteOptions();
    final TransactionOptions txnOptions = new TransactionOptions();

    return new TransactionDBContainer(txnOptions, writeOptions,
        columnFamilyHandles, txnDb, txnDbOptions, columnFamilyOptions, options);
  }

  private static class TransactionDBContainer
      extends DBContainer {
    private final TransactionOptions txnOptions;
    private final TransactionDB txnDb;
    private final TransactionDBOptions txnDbOptions;

    public TransactionDBContainer(
        final TransactionOptions txnOptions, final WriteOptions writeOptions,
        final List<ColumnFamilyHandle> columnFamilyHandles,
        final TransactionDB txnDb, final TransactionDBOptions txnDbOptions,
        final ColumnFamilyOptions columnFamilyOptions,
        final DBOptions options) {
      super(writeOptions, columnFamilyHandles, columnFamilyOptions,
          options);
      this.txnOptions = txnOptions;
      this.txnDb = txnDb;
      this.txnDbOptions = txnDbOptions;
    }

    @Override
    public Transaction beginTransaction() {
      return txnDb.beginTransaction(writeOptions, txnOptions);
    }

    @Override
    public Transaction beginTransaction(final WriteOptions writeOptions) {
      return txnDb.beginTransaction(writeOptions, txnOptions);
    }

    @Override
    public void close() {
      txnOptions.close();
      writeOptions.close();
      for(final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
        columnFamilyHandle.close();
      }
      txnDb.close();
      txnDbOptions.close();
      options.close();
    }
  }

}
