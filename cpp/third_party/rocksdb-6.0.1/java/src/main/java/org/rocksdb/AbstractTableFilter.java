package org.rocksdb;

/**
 * Base class for Table Filters.
 */
public abstract class AbstractTableFilter
    extends RocksCallbackObject implements TableFilter {

  protected AbstractTableFilter() {
    super();
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewTableFilter();
  }

  private native long createNewTableFilter();
}
