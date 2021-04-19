

##  4. Time



#### 4.1 Overview

In order to boost throughput, we model Milvus as a stream processing system. 



#### 4.5 T_safe





#### 4.1 Timestamp

Let's take a brief review of Hybrid Logical Clock (HLC). HLC uses 64bits timestamps which are composed of a 46-bits physical component (thought of as and always close to local wall time) and a 18-bits logical component (used to distinguish between events with the same physical component).

<img src="/Users/grt/Project/grt/milvus-distributed/docs/developer_guides/figs/hlc.png" width=400>

HLC's logical part is advanced on each request. The phsical part can be increased in two cases: 

A. when the local wall time is greater than HLC's physical part,

B. or the logical part overflows.

In either cases, the physical part will be updated, and the logical part will be set to 0.

Keep the physical part close to local wall time may face non-monotonic problems such as updates to POSIX time that could turn time backward. HLC avoids such problems, since if 'local wall time < HLC's physical part' holds, only case B is satisfied, thus montonicity is guaranteed.

Milvus does not support transaction, but it should gurantee the deterministic execution of the multi-way WAL. The timestamp attached to each request should

- have its physical part close to wall time (has an acceptable bounded error, a.k.a. uncertainty interval in transaction senarios),
- and be globally unique.

HLC leverages on physical clocks at nodes that are synchronized using the NTP. NTP usually maintain time to within tens of milliseconds over local networks in datacenter. Asymmetric routes and network congestion occasionally cause errors of hundreds of milliseconds. Both the normal time error and the spike are acceptable for Milvus use cases. 

The interface of Timestamp is as follows.

```
type timestamp struct {
  physical uint64 // 18-63 bits
  logical uint64  // 0-17 bits
}

type Timestamp uint64
```



#### 4.2 Timestamp Oracle

```go
type timestampOracle struct {
  client *etcd.Client // client of a reliable meta service, i.e. etcd client
  rootPath string // this timestampOracle's working root path on the reliable kv service
  saveInterval uint64
  lastSavedTime uint64
  tso Timestamp // monotonically increasing timestamp
}

func (tso *timestampOracle) GetTimestamp(count uint32) ([]Timestamp, error)

func (tso *timestampOracle) saveTimestamp() error
func (tso *timestampOracle) loadTimestamp() error
```



#### 4.2 Timestamp Allocator

```go
type TimestampAllocator struct {}

func (allocator *TimestampAllocator) Start() error
func (allocator *TimestampAllocator) Close() error
func (allocator *TimestampAllocator) Alloc(count uint32) ([]Timestamp, error)

func NewTimestampAllocator() *TimestampAllocator
```



###### 4.2.1 Batch Allocation of Timestamps

###### 4.2.2 Expiration of Timestamps





