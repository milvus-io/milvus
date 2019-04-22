package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.Environment;

import java.io.IOException;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;
import static org.assertj.core.api.Assertions.assertThat;

public class InfoLogLevelTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void testInfoLogLevel() throws RocksDBException,
      IOException {
    try (final RocksDB db =
             RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());
      db.flush(new FlushOptions().setWaitForFlush(true));
      assertThat(getLogContentsWithoutHeader()).isNotEmpty();
    }
  }

  @Test
  public void testFatalLogLevel() throws RocksDBException,
      IOException {
    try (final Options options = new Options().
        setCreateIfMissing(true).
        setInfoLogLevel(InfoLogLevel.FATAL_LEVEL);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      assertThat(options.infoLogLevel()).
          isEqualTo(InfoLogLevel.FATAL_LEVEL);
      db.put("key".getBytes(), "value".getBytes());
      // As InfoLogLevel is set to FATAL_LEVEL, here we expect the log
      // content to be empty.
      assertThat(getLogContentsWithoutHeader()).isEmpty();
    }
  }

  @Test
  public void testFatalLogLevelWithDBOptions()
      throws RocksDBException, IOException {
    try (final DBOptions dbOptions = new DBOptions().
        setInfoLogLevel(InfoLogLevel.FATAL_LEVEL);
         final Options options = new Options(dbOptions,
             new ColumnFamilyOptions()).
             setCreateIfMissing(true);
         final RocksDB db =
             RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(dbOptions.infoLogLevel()).
          isEqualTo(InfoLogLevel.FATAL_LEVEL);
      assertThat(options.infoLogLevel()).
          isEqualTo(InfoLogLevel.FATAL_LEVEL);
      db.put("key".getBytes(), "value".getBytes());
      assertThat(getLogContentsWithoutHeader()).isEmpty();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failIfIllegalByteValueProvided() {
    InfoLogLevel.getInfoLogLevel((byte) -1);
  }

  @Test
  public void valueOf() {
    assertThat(InfoLogLevel.valueOf("DEBUG_LEVEL")).
        isEqualTo(InfoLogLevel.DEBUG_LEVEL);
  }

  /**
   * Read LOG file contents into String.
   *
   * @return LOG file contents as String.
   * @throws IOException if file is not found.
   */
  private String getLogContentsWithoutHeader() throws IOException {
    final String separator = Environment.isWindows() ?
        "\n" : System.getProperty("line.separator");
    final String[] lines = new String(readAllBytes(get(
        dbFolder.getRoot().getAbsolutePath() + "/LOG"))).split(separator);

    int first_non_header = lines.length;
    // Identify the last line of the header
    for (int i = lines.length - 1; i >= 0; --i) {
      if (lines[i].indexOf("DB pointer") >= 0) {
        first_non_header = i + 1;
        break;
      }
    }
    StringBuilder builder = new StringBuilder();
    for (int i = first_non_header; i < lines.length; ++i) {
      builder.append(lines[i]).append(separator);
    }
    return builder.toString();
  }
}
