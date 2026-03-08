# pkoracle package

This package defines the interface and implementations for segments bloom filter sets of flushcommon metacache.

## BloomFilterSet

The basic implementation with segment statslog. A group of bloom filter to perdict whethe some pks exists in the segment.

## LazyPkStats

A wrapper for lazy loading PkStats. The inner PkStats could be added async.

*CANNOT* be used for growing segment.