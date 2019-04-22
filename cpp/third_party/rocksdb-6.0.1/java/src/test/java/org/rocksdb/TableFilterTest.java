package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TableFilterTest {

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void readOptions() throws RocksDBException {
    try (final DBOptions opt = new DBOptions().
            setCreateIfMissing(true).
            setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions()
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      // open database
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {

        try (final CfNameCollectionTableFilter cfNameCollectingTableFilter =
                 new CfNameCollectionTableFilter();
            final FlushOptions flushOptions =
                new FlushOptions().setWaitForFlush(true);
            final ReadOptions readOptions =
                 new ReadOptions().setTableFilter(cfNameCollectingTableFilter)) {

          db.put(columnFamilyHandles.get(0),
              "key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
          db.put(columnFamilyHandles.get(0),
              "key2".getBytes(UTF_8), "value2".getBytes(UTF_8));
          db.put(columnFamilyHandles.get(0),
              "key3".getBytes(UTF_8), "value3".getBytes(UTF_8));
          db.put(columnFamilyHandles.get(1),
              "key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
          db.put(columnFamilyHandles.get(1),
              "key2".getBytes(UTF_8), "value2".getBytes(UTF_8));
          db.put(columnFamilyHandles.get(1),
              "key3".getBytes(UTF_8), "value3".getBytes(UTF_8));

          db.flush(flushOptions, columnFamilyHandles);

          try (final RocksIterator iterator =
                   db.newIterator(columnFamilyHandles.get(0), readOptions)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
              iterator.key();
              iterator.value();
              iterator.next();
            }
          }

          try (final RocksIterator iterator =
                   db.newIterator(columnFamilyHandles.get(1), readOptions)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
              iterator.key();
              iterator.value();
              iterator.next();
            }
          }

          assertThat(cfNameCollectingTableFilter.cfNames.size()).isEqualTo(2);
          assertThat(cfNameCollectingTableFilter.cfNames.get(0))
              .isEqualTo(RocksDB.DEFAULT_COLUMN_FAMILY);
          assertThat(cfNameCollectingTableFilter.cfNames.get(1))
              .isEqualTo("new_cf".getBytes(UTF_8));
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  private static class CfNameCollectionTableFilter extends AbstractTableFilter {
    private final List<byte[]> cfNames = new ArrayList<>();

    @Override
    public boolean filter(final TableProperties tableProperties) {
      cfNames.add(tableProperties.getColumnFamilyName());
      return true;
    }
  }
}
