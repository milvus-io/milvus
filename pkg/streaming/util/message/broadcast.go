package message

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// newBroadcastHeaderFromProto creates a BroadcastHeader from proto.
func newBroadcastHeaderFromProto(proto *messagespb.BroadcastHeader) *BroadcastHeader {
	rks := make(typeutil.Set[ResourceKey], len(proto.GetResourceKeys()))
	for _, key := range proto.GetResourceKeys() {
		rks.Insert(NewResourceKeyFromProto(key))
	}
	return &BroadcastHeader{
		BroadcastID:  proto.GetBroadcastId(),
		VChannels:    proto.GetVchannels(),
		ResourceKeys: rks,
	}
}

type BroadcastHeader struct {
	BroadcastID  uint64
	VChannels    []string
	ResourceKeys typeutil.Set[ResourceKey]
}

// NewResourceKeyFromProto creates a ResourceKey from proto.
func NewResourceKeyFromProto(proto *messagespb.ResourceKey) ResourceKey {
	return ResourceKey{
		Domain: proto.Domain,
		Key:    proto.Key,
	}
}

// newProtoFromResourceKey creates a set of proto from ResourceKey.
func newProtoFromResourceKey(keys ...ResourceKey) []*messagespb.ResourceKey {
	deduplicated := typeutil.NewSet(keys...)
	protos := make([]*messagespb.ResourceKey, 0, len(keys))
	for key := range deduplicated {
		protos = append(protos, &messagespb.ResourceKey{
			Domain: key.Domain,
			Key:    key.Key,
		})
	}
	return protos
}

type ResourceKey struct {
	Domain messagespb.ResourceDomain
	Key    string
}

// NewImportJobIDResourceKey creates a key for import job resource.
func NewImportJobIDResourceKey(importJobID int64) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainImportJobID,
		Key:    strconv.FormatInt(importJobID, 10),
	}
}

// NewCollectionNameResourceKey creates a key for collection name resource.
func NewCollectionNameResourceKey(collectionName string) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainCollectionName,
		Key:    collectionName,
	}
}
