package message

import (
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// newBroadcastHeaderFromProto creates a BroadcastHeader from proto.
func newBroadcastHeaderFromProto(proto *messagespb.BroadcastHeader) *BroadcastHeader {
	rks := make(typeutil.Set[ResourceKey], len(proto.ResourceKeys))
	for _, key := range proto.ResourceKeys {
		rks.Insert(NewResourceKeyFromProto(key))
	}
	return &BroadcastHeader{
		BroadcastID:  proto.BroadcastId,
		VChannels:    proto.Vchannels,
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

func (rk *ResourceKey) IntoResourceKey() *messagespb.ResourceKey {
	return &messagespb.ResourceKey{
		Domain: rk.Domain,
		Key:    rk.Key,
	}
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

type BroadcastEvent = messagespb.BroadcastEvent

// UniqueKeyOfBroadcastEvent returns a unique key for a broadcast event.
func UniqueKeyOfBroadcastEvent(ev *BroadcastEvent) string {
	s, err := proto.Marshal(ev)
	if err != nil {
		panic(err)
	}
	return string(s)
}

// NewResourceKeyAckOneBroadcastEvent creates a broadcast event for acking one key.
func NewResourceKeyAckOneBroadcastEvent(rk ResourceKey) *BroadcastEvent {
	return &BroadcastEvent{
		Event: &messagespb.BroadcastEvent_ResourceKeyAckOne{
			ResourceKeyAckOne: &messagespb.BroadcastResourceKeyAckOne{
				ResourceKey: rk.IntoResourceKey(),
			},
		},
	}
}

// NewResourceKeyAckAllBroadcastEvent creates a broadcast event for ack all vchannel.
func NewResourceKeyAckAllBroadcastEvent(rk ResourceKey) *BroadcastEvent {
	return &BroadcastEvent{
		Event: &messagespb.BroadcastEvent_ResourceKeyAckAll{
			ResourceKeyAckAll: &messagespb.BroadcastResourceKeyAckAll{
				ResourceKey: rk.IntoResourceKey(),
			},
		},
	}
}
