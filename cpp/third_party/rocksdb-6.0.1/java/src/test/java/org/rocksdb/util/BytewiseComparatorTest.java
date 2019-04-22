// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import org.junit.Test;
import org.rocksdb.*;
import org.rocksdb.Comparator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

import static org.junit.Assert.*;

/**
 * This is a direct port of various C++
 * tests from db/comparator_db_test.cc
 * and some code to adapt it to RocksJava
 */
public class BytewiseComparatorTest {

  private List<String> source_strings = Arrays.asList("b", "d", "f", "h", "j", "l");
  private List<String> interleaving_strings = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m");

  /**
   * Open the database using the C++ BytewiseComparatorImpl
   * and test the results against our Java BytewiseComparator
   */
  @Test
  public void java_vs_cpp_bytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir = Files.createTempDirectory("comparator_db_test");
      try(final RocksDB db = openDatabase(dbDir,
          BuiltinComparator.BYTEWISE_COMPARATOR)) {

        final Random rnd = new Random(rand_seed);
        try(final ComparatorOptions copt2 = new ComparatorOptions();
            final Comparator comparator2 = new BytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(
              db,
              jComparator,
              rnd,
              8, 100, 3
          );
        }
      } finally {
        removeData(dbDir);
      }
    }
  }

  /**
   * Open the database using the Java BytewiseComparator
   * and test the results against another Java BytewiseComparator
   */
  @Test
  public void java_vs_java_bytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir = Files.createTempDirectory("comparator_db_test");
      try(final ComparatorOptions copt = new ComparatorOptions();
          final Comparator comparator = new BytewiseComparator(copt);
          final RocksDB db = openDatabase(dbDir, comparator)) {

        final Random rnd = new Random(rand_seed);
        try(final ComparatorOptions copt2 = new ComparatorOptions();
            final Comparator comparator2 = new BytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(
              db,
              jComparator,
              rnd,
              8, 100, 3
          );
        }
      } finally {
        removeData(dbDir);
      }
    }
  }

  /**
   * Open the database using the C++ BytewiseComparatorImpl
   * and test the results against our Java DirectBytewiseComparator
   */
  @Test
  public void java_vs_cpp_directBytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir = Files.createTempDirectory("comparator_db_test");
      try(final RocksDB db = openDatabase(dbDir,
          BuiltinComparator.BYTEWISE_COMPARATOR)) {

        final Random rnd = new Random(rand_seed);
        try(final ComparatorOptions copt2 = new ComparatorOptions();
            final DirectComparator comparator2 = new DirectBytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(
              db,
              jComparator,
              rnd,
              8, 100, 3
          );
        }
      } finally {
        removeData(dbDir);
      }
    }
  }

  /**
   * Open the database using the Java DirectBytewiseComparator
   * and test the results against another Java DirectBytewiseComparator
   */
  @Test
  public void java_vs_java_directBytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir = Files.createTempDirectory("comparator_db_test");
      try (final ComparatorOptions copt = new ComparatorOptions();
          final DirectComparator comparator = new DirectBytewiseComparator(copt);
          final RocksDB db = openDatabase(dbDir, comparator)) {

        final Random rnd = new Random(rand_seed);
        try(final ComparatorOptions copt2 = new ComparatorOptions();
            final DirectComparator comparator2 = new DirectBytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(
              db,
              jComparator,
              rnd,
              8, 100, 3
          );
        }
      } finally {
        removeData(dbDir);
      }
    }
  }

  /**
   * Open the database using the C++ ReverseBytewiseComparatorImpl
   * and test the results against our Java ReverseBytewiseComparator
   */
  @Test
  public void java_vs_cpp_reverseBytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir = Files.createTempDirectory("comparator_db_test");
      try(final RocksDB db = openDatabase(dbDir,
          BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR)) {

        final Random rnd = new Random(rand_seed);
        try(final ComparatorOptions copt2 = new ComparatorOptions();
            final Comparator comparator2 = new ReverseBytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(
              db,
              jComparator,
              rnd,
              8, 100, 3
          );
        }
      } finally {
        removeData(dbDir);
      }
    }
  }

  /**
   * Open the database using the Java ReverseBytewiseComparator
   * and test the results against another Java ReverseBytewiseComparator
   */
  @Test
  public void java_vs_java_reverseBytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir = Files.createTempDirectory("comparator_db_test");
      try (final ComparatorOptions copt = new ComparatorOptions();
           final Comparator comparator = new ReverseBytewiseComparator(copt);
           final RocksDB db = openDatabase(dbDir, comparator)) {

        final Random rnd = new Random(rand_seed);
        try(final ComparatorOptions copt2 = new ComparatorOptions();
            final Comparator comparator2 = new ReverseBytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(
              db,
              jComparator,
              rnd,
              8, 100, 3
          );
        }
      } finally {
        removeData(dbDir);
      }
    }
  }

  private void doRandomIterationTest(
      final RocksDB db, final java.util.Comparator<String> javaComparator,
      final Random rnd,
      final int num_writes, final int num_iter_ops,
      final int num_trigger_flush) throws RocksDBException {

    final TreeMap<String, String> map = new TreeMap<>(javaComparator);

    for (int i = 0; i < num_writes; i++) {
      if (num_trigger_flush > 0 && i != 0 && i % num_trigger_flush == 0) {
        db.flush(new FlushOptions());
      }

      final int type = rnd.nextInt(2);
      final int index = rnd.nextInt(source_strings.size());
      final String key = source_strings.get(index);
      switch (type) {
        case 0:
          // put
          map.put(key, key);
          db.put(new WriteOptions(), bytes(key), bytes(key));
          break;
        case 1:
          // delete
          if (map.containsKey(key)) {
            map.remove(key);
          }
          db.remove(new WriteOptions(), bytes(key));
          break;

        default:
          fail("Should not be able to generate random outside range 1..2");
      }
    }

    try(final RocksIterator iter = db.newIterator(new ReadOptions())) {
      final KVIter<String, String> result_iter = new KVIter(map);

      boolean is_valid = false;
      for (int i = 0; i < num_iter_ops; i++) {
        // Random walk and make sure iter and result_iter returns the
        // same key and value
        final int type = rnd.nextInt(7);
        iter.status();
        switch (type) {
          case 0:
            // Seek to First
            iter.seekToFirst();
            result_iter.seekToFirst();
            break;
          case 1:
            // Seek to last
            iter.seekToLast();
            result_iter.seekToLast();
            break;
          case 2: {
            // Seek to random (existing or non-existing) key
            final int key_idx = rnd.nextInt(interleaving_strings.size());
            final String key = interleaving_strings.get(key_idx);
            iter.seek(bytes(key));
            result_iter.seek(bytes(key));
            break;
          }
          case 3: {
            // SeekForPrev to random (existing or non-existing) key
            final int key_idx = rnd.nextInt(interleaving_strings.size());
            final String key = interleaving_strings.get(key_idx);
            iter.seekForPrev(bytes(key));
            result_iter.seekForPrev(bytes(key));
            break;
          }
          case 4:
            // Next
            if (is_valid) {
              iter.next();
              result_iter.next();
            } else {
              continue;
            }
            break;
          case 5:
            // Prev
            if (is_valid) {
              iter.prev();
              result_iter.prev();
            } else {
              continue;
            }
            break;
          default: {
            assert (type == 6);
            final int key_idx = rnd.nextInt(source_strings.size());
            final String key = source_strings.get(key_idx);
            final byte[] result = db.get(new ReadOptions(), bytes(key));
            if (!map.containsKey(key)) {
              assertNull(result);
            } else {
              assertArrayEquals(bytes(map.get(key)), result);
            }
            break;
          }
        }

        assertEquals(result_iter.isValid(), iter.isValid());

        is_valid = iter.isValid();

        if (is_valid) {
          assertArrayEquals(bytes(result_iter.key()), iter.key());

          //note that calling value on a non-valid iterator from the Java API
          //results in a SIGSEGV
          assertArrayEquals(bytes(result_iter.value()), iter.value());
        }
      }
    }
  }

  /**
   * Open the database using a C++ Comparator
   */
  private RocksDB openDatabase(
      final Path dbDir, final BuiltinComparator cppComparator)
      throws IOException, RocksDBException {
    final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(cppComparator);
    return RocksDB.open(options, dbDir.toAbsolutePath().toString());
  }

  /**
   * Open the database using a Java Comparator
   */
  private RocksDB openDatabase(
      final Path dbDir,
      final AbstractComparator<? extends AbstractSlice<?>> javaComparator)
      throws IOException, RocksDBException {
    final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(javaComparator);
    return RocksDB.open(options, dbDir.toAbsolutePath().toString());
  }

  private void closeDatabase(final RocksDB db) {
    db.close();
  }

  private void removeData(final Path dbDir) throws IOException {
    Files.walkFileTree(dbDir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(
          final Path file, final BasicFileAttributes attrs)
          throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(
          final Path dir, final IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private byte[] bytes(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private java.util.Comparator<String> toJavaComparator(
      final Comparator rocksComparator) {
    return new java.util.Comparator<String>() {
      @Override
      public int compare(final String s1, final String s2) {
        return rocksComparator.compare(new Slice(s1), new Slice(s2));
      }
    };
  }

  private java.util.Comparator<String> toJavaComparator(
      final DirectComparator rocksComparator) {
    return new java.util.Comparator<String>() {
      @Override
      public int compare(final String s1, final String s2) {
        return rocksComparator.compare(new DirectSlice(s1),
            new DirectSlice(s2));
      }
    };
  }

  private class KVIter<K, V> implements RocksIteratorInterface {

    private final List<Map.Entry<K, V>> entries;
    private final java.util.Comparator<? super K> comparator;
    private int offset = -1;

    private int lastPrefixMatchIdx = -1;
    private int lastPrefixMatch = 0;

    public KVIter(final TreeMap<K, V> map) {
      this.entries = new ArrayList<>();
      final Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
      while(iterator.hasNext()) {
        entries.add(iterator.next());
      }
      this.comparator = map.comparator();
    }


    @Override
    public boolean isValid() {
      return offset > -1 && offset < entries.size();
    }

    @Override
    public void seekToFirst() {
      offset = 0;
    }

    @Override
    public void seekToLast() {
      offset = entries.size() - 1;
    }

    @Override
    public void seek(final byte[] target) {
      for(offset = 0; offset < entries.size(); offset++) {
        if(comparator.compare(entries.get(offset).getKey(),
            (K)new String(target, StandardCharsets.UTF_8)) >= 0) {
          return;
        }
      }
    }

    @Override
    public void seekForPrev(final byte[] target) {
      for(offset = entries.size()-1; offset >= 0; offset--) {
        if(comparator.compare(entries.get(offset).getKey(),
            (K)new String(target, StandardCharsets.UTF_8)) <= 0) {
          return;
        }
      }
    }

    /**
     * Is `a` a prefix of `b`
     *
     * @return The length of the matching prefix, or 0 if it is not a prefix
     */
    private int isPrefix(final byte[] a, final byte[] b) {
      if(b.length >= a.length) {
        for(int i = 0; i < a.length; i++) {
          if(a[i] != b[i]) {
            return i;
          }
        }
        return a.length;
      } else {
        return 0;
      }
    }

    @Override
    public void next() {
      if(offset < entries.size()) {
        offset++;
      }
    }

    @Override
    public void prev() {
      if(offset >= 0) {
        offset--;
      }
    }

    @Override
    public void status() throws RocksDBException {
      if(offset < 0 || offset >= entries.size()) {
        throw new RocksDBException("Index out of bounds. Size is: " +
            entries.size() + ", offset is: " + offset);
      }
    }

    public K key() {
      if(!isValid()) {
        if(entries.isEmpty()) {
          return (K)"";
        } else if(offset == -1){
          return entries.get(0).getKey();
        } else if(offset == entries.size()) {
          return entries.get(offset - 1).getKey();
        } else {
          return (K)"";
        }
      } else {
        return entries.get(offset).getKey();
      }
    }

    public V value() {
      if(!isValid()) {
        return (V)"";
      } else {
        return entries.get(offset).getValue();
      }
    }
  }
}
