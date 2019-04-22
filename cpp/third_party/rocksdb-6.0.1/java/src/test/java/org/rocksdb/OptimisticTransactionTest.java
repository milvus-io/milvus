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

public class OptimisticTransactionTest extends AbstractTransactionTest {

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
        assertThat(txn.get(testCf, readOptions, k1)).isEqualTo(v1);
        txn.commit();
      }

      try(final Transaction txn2 = dbContainer.beginTransaction()) {
        try(final Transaction txn3 = dbContainer.beginTransaction()) {
          assertThat(txn3.getForUpdate(readOptions, testCf, k1, true)).isEqualTo(v1);

          // NOTE: txn2 updates k1, during txn3
          txn2.put(testCf, k1, v12);
          assertThat(txn2.get(testCf, readOptions, k1)).isEqualTo(v12);
          txn2.commit();

          try {
            txn3.commit(); // should cause an exception!
          } catch(final RocksDBException e) {
            assertThat(e.getStatus().getCode()).isSameAs(Status.Code.Busy);
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
        assertThat(txn.get(readOptions, k1)).isEqualTo(v1);
        txn.commit();
      }

      try(final Transaction txn2 = dbContainer.beginTransaction()) {
        try(final Transaction txn3 = dbContainer.beginTransaction()) {
          assertThat(txn3.getForUpdate(readOptions, k1, true)).isEqualTo(v1);

          // NOTE: txn2 updates k1, during txn3
          txn2.put(k1, v12);
          assertThat(txn2.get(readOptions, k1)).isEqualTo(v12);
          txn2.commit();

          try {
            txn3.commit(); // should cause an exception!
          } catch(final RocksDBException e) {
            assertThat(e.getStatus().getCode()).isSameAs(Status.Code.Busy);
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
          txn2.put(testCf, keys[0], otherValue);
          assertThat(txn2.get(testCf, readOptions, keys[0]))
              .isEqualTo(otherValue);
          txn2.commit();

          try {
            txn3.commit(); // should cause an exception!
          } catch(final RocksDBException e) {
            assertThat(e.getStatus().getCode()).isSameAs(Status.Code.Busy);
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
          txn2.put(keys[0], otherValue);
          assertThat(txn2.get(readOptions, keys[0]))
              .isEqualTo(otherValue);
          txn2.commit();

          try {
            txn3.commit(); // should cause an exception!
          } catch(final RocksDBException e) {
            assertThat(e.getStatus().getCode()).isSameAs(Status.Code.Busy);
            return;
          }
        }
      }

      fail("Expected an exception for put after getForUpdate from conflicting" +
          "transactions");
    }
  }

  @Test
  public void undoGetForUpdate_cf_conflict() throws RocksDBException {
    final byte k1[] = "key1".getBytes(UTF_8);
    final byte v1[] = "value1".getBytes(UTF_8);
    final byte v12[] = "value12".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions()) {
      final ColumnFamilyHandle testCf = dbContainer.getTestColumnFamily();

      try(final Transaction txn = dbContainer.beginTransaction()) {
        txn.put(testCf, k1, v1);
        assertThat(txn.get(testCf, readOptions, k1)).isEqualTo(v1);
        txn.commit();
      }

      try(final Transaction txn2 = dbContainer.beginTransaction()) {
        try(final Transaction txn3 = dbContainer.beginTransaction()) {
          assertThat(txn3.getForUpdate(readOptions, testCf, k1, true)).isEqualTo(v1);

          // undo the getForUpdate
          txn3.undoGetForUpdate(testCf, k1);

          // NOTE: txn2 updates k1, during txn3
          txn2.put(testCf, k1, v12);
          assertThat(txn2.get(testCf, readOptions, k1)).isEqualTo(v12);
          txn2.commit();

          // should not cause an exception
          // because we undid the getForUpdate above!
          txn3.commit();
        }
      }
    }
  }

  @Test
  public void undoGetForUpdate_conflict() throws RocksDBException {
    final byte k1[] = "key1".getBytes(UTF_8);
    final byte v1[] = "value1".getBytes(UTF_8);
    final byte v12[] = "value12".getBytes(UTF_8);
    try(final DBContainer dbContainer = startDb();
        final ReadOptions readOptions = new ReadOptions()) {

      try(final Transaction txn = dbContainer.beginTransaction()) {
        txn.put(k1, v1);
        assertThat(txn.get(readOptions, k1)).isEqualTo(v1);
        txn.commit();
      }

      try(final Transaction txn2 = dbContainer.beginTransaction()) {
        try(final Transaction txn3 = dbContainer.beginTransaction()) {
          assertThat(txn3.getForUpdate(readOptions, k1, true)).isEqualTo(v1);

          // undo the getForUpdate
          txn3.undoGetForUpdate(k1);

          // NOTE: txn2 updates k1, during txn3
          txn2.put(k1, v12);
          assertThat(txn2.get(readOptions, k1)).isEqualTo(v12);
          txn2.commit();

          // should not cause an exception
          // because we undid the getForUpdate above!
          txn3.commit();
        }
      }
    }
  }

  @Test
  public void name() throws RocksDBException {
    try(final DBContainer dbContainer = startDb();
        final Transaction txn = dbContainer.beginTransaction()) {
      assertThat(txn.getName()).isEmpty();
      final String name = "my-transaction-" + rand.nextLong();

      try {
        txn.setName(name);
      } catch(final RocksDBException e) {
         assertThat(e.getStatus().getCode() == Status.Code.InvalidArgument);
        return;
      }

      fail("Optimistic transactions cannot be named.");
    }
  }

  @Override
  public OptimisticTransactionDBContainer startDb()
      throws RocksDBException {
    final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

    final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor(TXN_TEST_COLUMN_FAMILY,
                columnFamilyOptions));
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    final OptimisticTransactionDB optimisticTxnDb;
    try {
      optimisticTxnDb = OptimisticTransactionDB.open(
          options, dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors, columnFamilyHandles);
    } catch(final RocksDBException e) {
      columnFamilyOptions.close();
      options.close();
      throw e;
    }

    final WriteOptions writeOptions = new WriteOptions();
    final OptimisticTransactionOptions optimisticTxnOptions =
             new OptimisticTransactionOptions();

    return new OptimisticTransactionDBContainer(optimisticTxnOptions,
        writeOptions, columnFamilyHandles, optimisticTxnDb, columnFamilyOptions,
        options);
  }

  private static class OptimisticTransactionDBContainer
      extends DBContainer {

    private final OptimisticTransactionOptions optimisticTxnOptions;
    private final OptimisticTransactionDB optimisticTxnDb;

    public OptimisticTransactionDBContainer(
        final OptimisticTransactionOptions optimisticTxnOptions,
        final WriteOptions writeOptions,
        final List<ColumnFamilyHandle> columnFamilyHandles,
        final OptimisticTransactionDB optimisticTxnDb,
        final ColumnFamilyOptions columnFamilyOptions,
        final DBOptions options) {
      super(writeOptions, columnFamilyHandles, columnFamilyOptions,
          options);
      this.optimisticTxnOptions = optimisticTxnOptions;
      this.optimisticTxnDb = optimisticTxnDb;
    }

    @Override
    public Transaction beginTransaction() {
      return optimisticTxnDb.beginTransaction(writeOptions,
          optimisticTxnOptions);
    }

    @Override
    public Transaction beginTransaction(final WriteOptions writeOptions) {
      return optimisticTxnDb.beginTransaction(writeOptions,
          optimisticTxnOptions);
    }

    @Override
    public void close() {
      optimisticTxnOptions.close();
      writeOptions.close();
      for(final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
        columnFamilyHandle.close();
      }
      optimisticTxnDb.close();
      options.close();
    }
  }
}
