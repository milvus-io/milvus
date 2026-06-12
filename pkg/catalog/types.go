package catalog

import "github.com/milvus-io/milvus/pkg/v3/util/typeutil"

type (
	Timestamp = typeutil.Timestamp
	UniqueID  = typeutil.UniqueID
)

type DatabaseRef struct {
	ID   UniqueID
	Name string
}

type CollectionRef struct {
	Database DatabaseRef
	ID       UniqueID
	Name     string
}

type PartitionRef struct {
	Collection CollectionRef
	ID         UniqueID
	Name       string
}

type AliasRef struct {
	Database DatabaseRef
	Name     string
}

type SegmentRef struct {
	CollectionID UniqueID
	PartitionID  UniqueID
	SegmentID    UniqueID
}

type IndexRef struct {
	CollectionID UniqueID
	IndexID      UniqueID
	Name         string
}

type SegmentIndexRef struct {
	CollectionID UniqueID
	PartitionID  UniqueID
	SegmentID    UniqueID
	BuildID      UniqueID
}

type Version struct {
	Token string
}

type Epoch struct {
	LeaderID string
	Value    uint64
}

type ReadOptions struct {
	At      Timestamp
	StaleOK bool
}

type WriteOptions struct {
	RequestID string
	Timestamp Timestamp
	Expected  *Version
	Epoch     *Epoch
}

type AlterType int32

const (
	ADD AlterType = iota
	DELETE
	MODIFY
)
