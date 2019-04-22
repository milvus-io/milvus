---
title: PinnableSlice; less memcpy with point lookups
layout: post
author: maysamyabandeh
category: blog
---

The classic API for [DB::Get](https://github.com/facebook/rocksdb/blob/9e583711144f580390ce21a49a8ceacca338fcd5/include/rocksdb/db.h#L310) receives a std::string as argument to which it will copy the value. The memcpy overhead could be non-trivial when the value is large. The [new API](https://github.com/facebook/rocksdb/blob/9e583711144f580390ce21a49a8ceacca338fcd5/include/rocksdb/db.h#L322) receives a PinnableSlice instead, which avoids memcpy in most of the cases.

### What is PinnableSlice?

Similarly to Slice, PinnableSlice refers to some in-memory data so it does not incur the memcpy cost. To ensure that the data will not be erased while it is being processed by the user, PinnableSlice, as its name suggests, has the data pinned in memory. The pinned data are released when PinnableSlice object is destructed or when ::Reset is invoked explicitly on it.

### How good is it?

Here are the improvements in throughput for an [in-memory benchmark](https://github.com/facebook/rocksdb/pull/1756#issuecomment-286201693):
* value 1k byte: 14%
* value 10k byte: 34%

### Any limitations?

PinnableSlice tries to avoid memcpy as much as possible. The primary gain is when reading large values from the block cache. There are however cases that it would still have to copy the data into its internal buffer. The reason is mainly the complexity of implementation and if there is enough motivation on the application side. the scope of PinnableSlice could be extended to such cases too. These include:
* Merged values
* Reads from memtables

### How to use it?

```cpp
PinnableSlice pinnable_val;
while (!stopped) { 
   auto s = db->Get(opt, cf, key, &pinnable_val);
   // ... use it
   pinnable_val.Reset(); // then release it immediately
}
```

You can also [initialize the internal buffer](https://github.com/facebook/rocksdb/blob/9e583711144f580390ce21a49a8ceacca338fcd5/include/rocksdb/db.h#L314) of PinnableSlice by passing your own string in the constructor. [simple_example.cc](https://github.com/facebook/rocksdb/blob/master/examples/simple_example.cc) demonstrates that with more examples.
