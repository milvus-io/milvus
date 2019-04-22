// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.rocksdb.util.CapturingWriteBatchHandler;
import org.rocksdb.util.CapturingWriteBatchHandler.Event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.util.CapturingWriteBatchHandler.Action.*;


public class WriteBatchHandlerTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void writeBatchHandler() throws RocksDBException {
    // setup test data
    final List<Event> testEvents = Arrays.asList(
        new Event(DELETE, "k0".getBytes(), null),
        new Event(PUT, "k1".getBytes(), "v1".getBytes()),
        new Event(PUT, "k2".getBytes(), "v2".getBytes()),
        new Event(PUT, "k3".getBytes(), "v3".getBytes()),
        new Event(LOG, null, "log1".getBytes()),
        new Event(MERGE, "k2".getBytes(), "v22".getBytes()),
        new Event(DELETE, "k3".getBytes(), null)
    );

    // load test data to the write batch
    try (final WriteBatch batch = new WriteBatch()) {
      for (final Event testEvent : testEvents) {
        switch (testEvent.action) {

          case PUT:
            batch.put(testEvent.key, testEvent.value);
            break;

          case MERGE:
            batch.merge(testEvent.key, testEvent.value);
            break;

          case DELETE:
            batch.remove(testEvent.key);
            break;

          case LOG:
            batch.putLogData(testEvent.value);
            break;
        }
      }

      // attempt to read test data back from the WriteBatch by iterating
      // with a handler
      try (final CapturingWriteBatchHandler handler =
               new CapturingWriteBatchHandler()) {
        batch.iterate(handler);

        // compare the results to the test data
        final List<Event> actualEvents =
            handler.getEvents();
        assertThat(testEvents.size()).isSameAs(actualEvents.size());

        assertThat(testEvents).isEqualTo(actualEvents);
      }
    }
  }
}
