# Hybrid Timestamp in Milvus

In chapter [Milvus TimeSync Mechanism](./milvus_timesync_en.md), we have already known why we need TSO in Milvus. Milvus
uses the [TiKV's](https://github.com/tikv/tikv) implementation into TSO. So if you are interested in how TSO is
implemented, you can look into the official documentation of TiKV.

This chapter will only introduce two points:

1. the organization of hybrid TSO in Milvus;
2. how should we parse the hybrid TSO;

## The Organization of TSO

The type of TSO is `uint64`. As shown in the figure below, TSO was organized by two parts:

1. physical part;
2. logical part;

The front 46 bits is of physical part, and the last 18 bits is of logical part.

Note, physical part is the UTC time in Milliseconds.

![Timestamp struct](./graphs/time_stamp_struct.jpg)

For some users such as DBAs, they would want to sort the operations and list them in UTC time order.

Actually, we can use the TSO order to sort the `Insert` operations or `Delete` operations.

So the question becomes how we get the UTC time from TSO.

As we have described above, the physical part consists of the front 46 bits of TSO.

So given a TSO which is returned by `Insert` or `Delete`, we can directly shift the left 18 bits to get the UTC time.

For example in Golang:

```go
const (
	logicalBits     = 18
	logicalBitsMask = (1 << logicalBits) - 1
)

// ParseTS parses the ts to (physical,logical).
func ParseTS(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}
```

In Python:

```python
>>> import datetime
>>> LOGICAL_BITS = 18
>>> LOGICAL_BITS_MASK = (1 << LOGICAL_BITS) - 1
>>> def parse_ts(ts):
...     logical = ts & LOGICAL_BITS_MASK
...     physical = ts >> LOGICAL_BITS
...     return physical, logical
... 
>>> ts = 429164525386203142
>>> utc_ts_in_milliseconds, _ = parse_ts(ts)
>>> d = datetime.datetime.fromtimestamp(utc_ts_in_milliseconds / 1000.0)
>>> d.strftime('%Y-%m-%d %H:%M:%S')
'2021-11-17 15:05:41'
>>> 
```
