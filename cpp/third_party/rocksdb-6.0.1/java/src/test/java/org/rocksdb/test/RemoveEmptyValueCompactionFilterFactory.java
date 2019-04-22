package org.rocksdb.test;

import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;
import org.rocksdb.RemoveEmptyValueCompactionFilter;

/**
 * Simple CompactionFilterFactory class used in tests. Generates RemoveEmptyValueCompactionFilters.
 */
public class RemoveEmptyValueCompactionFilterFactory extends AbstractCompactionFilterFactory<RemoveEmptyValueCompactionFilter> {
    @Override
    public RemoveEmptyValueCompactionFilter createCompactionFilter(final AbstractCompactionFilter.Context context) {
        return new RemoveEmptyValueCompactionFilter();
    }

    @Override
    public String name() {
        return "RemoveEmptyValueCompactionFilterFactory";
    }
}
