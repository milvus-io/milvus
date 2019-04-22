package org.rocksdb.util;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A simple WriteBatch Handler which adds a record
 * of each event that it receives to a list
 */
public class CapturingWriteBatchHandler extends WriteBatch.Handler {

  private final List<Event> events = new ArrayList<>();

  /**
   * Returns a copy of the current events list
   *
   * @return a list of the events which have happened upto now
   */
  public List<Event> getEvents() {
    return new ArrayList<>(events);
  }

  @Override
  public void put(final int columnFamilyId, final byte[] key,
                  final byte[] value) {
    events.add(new Event(Action.PUT, columnFamilyId, key, value));
  }

  @Override
  public void put(final byte[] key, final byte[] value) {
    events.add(new Event(Action.PUT, key, value));
  }

  @Override
  public void merge(final int columnFamilyId, final byte[] key,
                    final byte[] value) {
    events.add(new Event(Action.MERGE, columnFamilyId, key, value));
  }

  @Override
  public void merge(final byte[] key, final byte[] value) {
    events.add(new Event(Action.MERGE, key, value));
  }

  @Override
  public void delete(final int columnFamilyId, final byte[] key) {
    events.add(new Event(Action.DELETE, columnFamilyId, key, (byte[])null));
  }

  @Override
  public void delete(final byte[] key) {
    events.add(new Event(Action.DELETE, key, (byte[])null));
  }

  @Override
  public void singleDelete(final int columnFamilyId, final byte[] key) {
    events.add(new Event(Action.SINGLE_DELETE,
        columnFamilyId, key, (byte[])null));
  }

  @Override
  public void singleDelete(final byte[] key) {
    events.add(new Event(Action.SINGLE_DELETE, key, (byte[])null));
  }

  @Override
  public void deleteRange(final int columnFamilyId, final byte[] beginKey,
                          final byte[] endKey) {
    events.add(new Event(Action.DELETE_RANGE, columnFamilyId, beginKey,
        endKey));
  }

  @Override
  public void deleteRange(final byte[] beginKey, final byte[] endKey) {
    events.add(new Event(Action.DELETE_RANGE, beginKey, endKey));
  }

  @Override
  public void logData(final byte[] blob) {
    events.add(new Event(Action.LOG, (byte[])null, blob));
  }

  @Override
  public void putBlobIndex(final int columnFamilyId, final byte[] key,
                           final byte[] value) {
    events.add(new Event(Action.PUT_BLOB_INDEX, key, value));
  }

  @Override
  public void markBeginPrepare() throws RocksDBException {
    events.add(new Event(Action.MARK_BEGIN_PREPARE, (byte[])null,
        (byte[])null));
  }

  @Override
  public void markEndPrepare(final byte[] xid) throws RocksDBException {
    events.add(new Event(Action.MARK_END_PREPARE, (byte[])null,
        (byte[])null));
  }

  @Override
  public void markNoop(final boolean emptyBatch) throws RocksDBException {
    events.add(new Event(Action.MARK_NOOP, (byte[])null, (byte[])null));
  }

  @Override
  public void markRollback(final byte[] xid) throws RocksDBException {
    events.add(new Event(Action.MARK_ROLLBACK, (byte[])null, (byte[])null));
  }

  @Override
  public void markCommit(final byte[] xid) throws RocksDBException {
    events.add(new Event(Action.MARK_COMMIT, (byte[])null, (byte[])null));
  }

  public static class Event {
    public final Action action;
    public final int columnFamilyId;
    public final byte[] key;
    public final byte[] value;

    public Event(final Action action, final byte[] key, final byte[] value) {
      this(action, 0, key, value);
    }

    public Event(final Action action, final int columnFamilyId, final byte[] key,
        final byte[] value) {
      this.action = action;
      this.columnFamilyId = columnFamilyId;
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Event event = (Event) o;
      return columnFamilyId == event.columnFamilyId &&
          action == event.action &&
          ((key == null && event.key == null)
              || Arrays.equals(key, event.key)) &&
          ((value == null && event.value == null)
              || Arrays.equals(value, event.value));
    }

    @Override
    public int hashCode() {

      return Objects.hash(action, columnFamilyId, key, value);
    }
  }

  /**
   * Enumeration of Write Batch
   * event actions
   */
  public enum Action {
    PUT, MERGE, DELETE, SINGLE_DELETE, DELETE_RANGE, LOG, PUT_BLOB_INDEX,
    MARK_BEGIN_PREPARE, MARK_END_PREPARE, MARK_NOOP, MARK_COMMIT,
    MARK_ROLLBACK }
}
