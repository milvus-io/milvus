package model

import "github.com/milvus-io/milvus/pkg/proto/querypb"

type CollectionLoadInfo struct {
	CollectionID         int64
	PartitionIDs         []int64
	ReleasedPartitionIDs []int64
	LoadType             querypb.LoadType
	LoadPercentage       int64
	Status               querypb.LoadStatus
	ReplicaNumber        int32
	FieldIndexID         map[int64]int64
}

type PartitionLoadInfo struct {
	CollectionID   int64
	PartitionID    int64
	LoadType       querypb.LoadType
	LoadPercentage int64
	Status         querypb.LoadStatus
	ReplicaNumber  int32
	FieldIndexID   map[int64]int64
}
