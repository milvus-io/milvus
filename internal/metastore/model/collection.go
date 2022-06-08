package model

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Collection struct {
	TenantID                 string
	CollectionID             int64
	Partitions               []*Partition
	Name                     string
	Description              string
	AutoID                   bool
	Fields                   []*Field
	FieldIndexes             []*Index
	VirtualChannelNames      []string
	PhysicalChannelNames     []string
	ShardsNum                int32
	StartPositions           []*commonpb.KeyDataPair
	CreateTime               uint64
	ConsistencyLevel         commonpb.ConsistencyLevel
	Aliases                  []string
	BuiltImmutableAlias2Name typeutil.ImmutablemapString2string
	AliasTimeStamp           typeutil.Timestamp
	Extra                    map[string]string // extra kvs
}
