package policy

import "time"

type PolicyName string

var (
	PolicyNamePartitionNotFound      PolicyName = "partition_not_found"
	PolicyNamePartitionRemoved       PolicyName = "partition_removed"
	PolicyNameCollectionRemoved      PolicyName = "collection_removed"
	PolicyNameRecover                PolicyName = "recover"
	PolicyNameFenced                 PolicyName = "fenced"
	PolicyNameForce                  PolicyName = "force"
	PolicyNameCapacity               PolicyName = "capacity"
	PolicyNameBinlogNumber           PolicyName = "binlog_number"
	PolicyNameLifetime               PolicyName = "lifetime"
	PolicyNameIdle                   PolicyName = "idle"
	PolicyNameGrowingSegmentBytesHWM PolicyName = "growing_bytes_hwm"
	PolicyNameNodeMemory             PolicyName = "node_memory"
)

// PolicyPartitionNotFound returns a SealPolicy for partition not found.
func PolicyParitionNotFound() SealPolicy {
	return SealPolicy{
		Policy: PolicyNamePartitionNotFound,
		Extra:  nil,
	}
}

// PolicyPartitionRemoved returns a SealPolicy for partition removed.
func PolicyPartitionRemoved() SealPolicy {
	return SealPolicy{
		Policy: PolicyNamePartitionRemoved,
		Extra:  nil,
	}
}

// PolicyCollectionRemoved returns a SealPolicy for collection removed.
func PolicyCollectionRemoved() SealPolicy {
	return SealPolicy{
		Policy: PolicyNameCollectionRemoved,
		Extra:  nil,
	}
}

// PolicyRecover returns a SealPolicy for recover.
func PolicyRecover() SealPolicy {
	return SealPolicy{
		Policy: PolicyNameRecover,
		Extra:  nil,
	}
}

// PolicyFenced returns a SealPolicy for fenced.
func PolicyFenced(timetick uint64) SealPolicy {
	return SealPolicy{
		Policy: PolicyNameFenced,
		Extra:  sealFenced{TimeTick: timetick},
	}
}

// PolicyCapacity returns a SealPolicy for capacity.
func PolicyCapacity() SealPolicy {
	return SealPolicy{
		Policy: PolicyNameCapacity,
		Extra:  nil,
	}
}

// PolicyBinlogNumber returns a SealPolicy for binlog number.
func PolicyBinlogNumber(binlogNumberLimit uint64) SealPolicy {
	return SealPolicy{
		Policy: PolicyNameBinlogNumber,
		Extra:  sealByBinlogFileExtraInfo{BinLogNumberLimit: binlogNumberLimit},
	}
}

// PolicyLifetime returns a SealPolicy for lifetime.
func PolicyLifetime(maxLifetime time.Duration) SealPolicy {
	return SealPolicy{
		Policy: PolicyNameLifetime,
		Extra:  sealByLifetimeExtraInfo{MaxLifetime: maxLifetime},
	}
}

// PolicyIdle returns a SealPolicy for idle.
func PolicyIdle(idleTime time.Duration, minimalSize uint64) SealPolicy {
	return SealPolicy{
		Policy: PolicyNameIdle,
		Extra:  sealByIdleTimeExtraInfo{IdleTime: idleTime, MinimalSize: minimalSize},
	}
}

// PolicyGrowingSegmentBytesHWM returns a SealPolicy for growing segment bytes hwm.
func PolicyGrowingSegmentBytesHWM(totalBytes uint64) SealPolicy {
	return SealPolicy{
		Policy: PolicyNameGrowingSegmentBytesHWM,
		Extra: sealByGrowingSegmentBytesHWM{
			TotalBytes: totalBytes,
		},
	}
}

// PolicyNodeMemory returns a SealPolicy for node memory.
func PolicyNodeMemory(usedRatio float64) SealPolicy {
	return SealPolicy{
		Policy: PolicyNameNodeMemory,
		Extra: nodeMemory{
			UsedRatio: usedRatio,
		},
	}
}

// PolicyRecover returns a SealPolicy for recover.
type SealPolicy struct {
	Policy PolicyName
	Extra  interface{}
}

type sealFenced struct {
	TimeTick uint64
}

// sealByBinlogFileExtraInfo is the extra info of the seal by binlog file number policy.
type sealByBinlogFileExtraInfo struct {
	BinLogNumberLimit uint64
}

// sealByLifetimeExtraInfo is the extra info of the seal by lifetime policy.
type sealByLifetimeExtraInfo struct {
	MaxLifetime time.Duration
}

// sealByGrowingSegmentBytesHWM is the extra info of the seal by growing segment bytes hwm policy.
type sealByGrowingSegmentBytesHWM struct {
	TotalBytes uint64
}

// nodeMemory is the extra info of the seal by node memory policy.
type nodeMemory struct {
	UsedRatio float64
}

// sealByIdleTimeExtraInfo is the extra info of the seal by idle time policy.
type sealByIdleTimeExtraInfo struct {
	IdleTime    time.Duration
	MinimalSize uint64
}
