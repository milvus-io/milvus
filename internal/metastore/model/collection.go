// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"slices"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// TODO: These collection is dirty implementation and easy to be broken, we should drop it in the future.
type Collection struct {
	TenantID             string
	DBID                 int64
	CollectionID         int64
	Partitions           []*Partition
	Name                 string
	DBName               string
	Description          string
	AutoID               bool
	Fields               []*Field
	StructArrayFields    []*StructArrayField
	Functions            []*Function
	VirtualChannelNames  []string
	PhysicalChannelNames []string
	ShardsNum            int32
	StartPositions       []*commonpb.KeyDataPair
	CreateTime           uint64
	ConsistencyLevel     commonpb.ConsistencyLevel
	Aliases              []string // TODO: deprecate this.
	Properties           []*commonpb.KeyValuePair
	State                pb.CollectionState
	EnableDynamicField   bool
	EnableNamespace      bool
	UpdateTimestamp      uint64
	SchemaVersion        int32
	ShardInfos           map[string]*ShardInfo
	RoutingMode          schemapb.RoutingMode // how a routing key maps to a shard, RoutingModeHash for legacy collections.
	FileResourceIds      []int64
	ExternalSource       string
	ExternalSpec         string
}

// RoutingKeyRange is one half-open [Lower, Upper) routing-key range a shard
// owns. A nil bound is unbounded (Lower nil = -inf, Upper nil = +inf).
type RoutingKeyRange struct {
	Lower []byte
	Upper []byte
}

type ShardInfo struct {
	PChannelName         string              // the pchannel name of the shard, it is the same with the physical channel name.
	VChannelName         string              // the vchannel name of the shard, it is the same with the virtual channel name.
	LastTruncateTimeTick uint64              // the last truncate time tick of the shard, if the shard is not truncated, the value is 0.
	State                schemapb.ShardState // the lifecycle state during shard split, ShardNormal by default.
	// SourceVChannels, on an in-progress split target (ShardCreating, kept until
	// the sources are released), are the source vchannels it is being carved
	// from, so consumers can group a source with exactly its own targets. Empty
	// otherwise.
	//
	// A set rather than a single name: splitting one shard in two gives each
	// target one source, but rehashing a collection to an arbitrary shard count
	// gives every target a slice of every source. A consumer must therefore
	// treat a target as fully materialized only once every source in this list
	// has been accounted for.
	SourceVChannels []string
	// FrontingSourceVChannel is the one source whose delegator fronts this
	// target's reads while the split is in flight. Empty for a shard that is not
	// an in-progress split target.
	//
	// It has to survive a round trip through meta, not just live in the fence
	// message: a querynode that restarts mid-window rebuilds its children from
	// the collection's shard infos, and if this were lost every source would
	// re-front every target and each target's rows would be returned once per
	// source.
	FrontingSourceVChannel string
	// Ranges is the range-routing predicate of the shard: the half-open key
	// ranges it owns. Usually one range; more than one after a carve-out leaves a
	// shard with disjoint pieces. Empty for a legacy hash-routed shard. Only
	// meaningful when the collection's RoutingMode is RoutingModeRange.
	Ranges []RoutingKeyRange
	// Buckets is the hash-routing predicate of the shard: the hash buckets it
	// owns, each matching the keys where hash(pk) % Modulus == Remainder.
	//
	// Empty for a hash-routed collection that has never been split, which keeps
	// routing on the legacy "hash(pk) % shardNum" rule. Once ANY shard of a
	// collection carries a bucket, every routable shard must, or the derived
	// table has a hole and the write path cannot be built — so a split commits
	// the buckets of the untouched shards along with its own.
	//
	// Only meaningful when the collection's RoutingMode is RoutingModeHash.
	Buckets []HashBucket
}

// HashBucket is one piece of a shard's hash-routing predicate: the shard owns
// the keys where hash(key) % Modulus == Remainder.
type HashBucket struct {
	Modulus   uint64
	Remainder uint64
}

// hashBucketsFromPB converts the hash-routing buckets of a
// schemapb.CollectionShardInfo into the model representation.
func hashBucketsFromPB(buckets []*schemapb.HashBucket) []HashBucket {
	if len(buckets) == 0 {
		return nil
	}
	out := make([]HashBucket, len(buckets))
	for i, b := range buckets {
		out[i] = HashBucket{Modulus: b.GetModulus(), Remainder: b.GetRemainder()}
	}
	return out
}

// cloneRoutingKeyRanges deep-copies a shard's range-routing predicate.
func cloneRoutingKeyRanges(ranges []RoutingKeyRange) []RoutingKeyRange {
	if ranges == nil {
		return nil
	}
	out := make([]RoutingKeyRange, len(ranges))
	for i, r := range ranges {
		out[i] = RoutingKeyRange{Lower: slices.Clone(r.Lower), Upper: slices.Clone(r.Upper)}
	}
	return out
}

// routingKeyRangesFromPB converts the range-routing ranges of a
// schemapb.CollectionShardInfo into the model representation.
func routingKeyRangesFromPB(ranges []*schemapb.RoutingKeyRange) []RoutingKeyRange {
	if len(ranges) == 0 {
		return nil
	}
	out := make([]RoutingKeyRange, len(ranges))
	for i, r := range ranges {
		out[i] = RoutingKeyRange{Lower: r.GetLower(), Upper: r.GetUpper()}
	}
	return out
}

// routableShardCount counts the shards a key can currently be routed to, which
// is what a collection's shard count means to a user.
//
// A split source is excluded from the moment it is fenced: it is Splitting (and
// later Dropped) and owns no key range, so it is no longer one of the
// collection's shards even though its vchannel lingers until its data has been
// moved. Counting vchannels instead would report N+M during a rehash — every
// source and every target at once — which is a number the collection never
// actually has.
func routableShardCount(infos []*schemapb.CollectionShardInfo) int32 {
	var count int32
	for _, info := range infos {
		switch info.GetState() {
		case schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardCreating:
			count++
		}
	}
	return count
}

// ToPB builds the schemapb.CollectionShardInfo of a shard; the routing oneof is
// left unset when the shard owns no predicate (e.g. a hash-routed shard of a
// collection that has never been split, or a fenced/dropped split source).
//
// The oneof holds one variant, so a shard cannot carry both; ranges win when a
// caller somehow set both, matching RoutingMode being the collection-wide
// authority on which variant is meaningful.
func (s *ShardInfo) ToPB() *schemapb.CollectionShardInfo {
	si := &schemapb.CollectionShardInfo{
		LastTruncateTimeTick:   s.LastTruncateTimeTick,
		State:                  s.State,
		VchannelName:           s.VChannelName,
		SourceVchannels:        s.SourceVChannels,
		FrontingSourceVchannel: s.FrontingSourceVChannel,
	}
	switch {
	case len(s.Ranges) > 0:
		pbRanges := make([]*schemapb.RoutingKeyRange, len(s.Ranges))
		for i, r := range s.Ranges {
			pbRanges[i] = &schemapb.RoutingKeyRange{Lower: r.Lower, Upper: r.Upper}
		}
		si.Routing = &schemapb.CollectionShardInfo_RangeRouting{RangeRouting: &schemapb.RangeRouting{Ranges: pbRanges}}
	case len(s.Buckets) > 0:
		pbBuckets := make([]*schemapb.HashBucket, len(s.Buckets))
		for i, b := range s.Buckets {
			pbBuckets[i] = &schemapb.HashBucket{Modulus: b.Modulus, Remainder: b.Remainder}
		}
		si.Routing = &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: pbBuckets}}
	}
	return si
}

// shardInfoFromPB builds the model ShardInfo of one shard from its persisted /
// wire form. Both routing variants are read; which one is meaningful is decided
// by the collection's RoutingMode, not here.
func shardInfoFromPB(vchannel, pchannel string, si *schemapb.CollectionShardInfo) *ShardInfo {
	return &ShardInfo{
		VChannelName:           vchannel,
		PChannelName:           pchannel,
		LastTruncateTimeTick:   si.GetLastTruncateTimeTick(),
		State:                  si.GetState(),
		SourceVChannels:        slices.Clone(si.GetSourceVchannels()),
		FrontingSourceVChannel: si.GetFrontingSourceVchannel(),
		Ranges:                 routingKeyRangesFromPB(si.GetRangeRouting().GetRanges()),
		Buckets:                hashBucketsFromPB(si.GetHashRouting().GetBuckets()),
	}
}

func (c *Collection) Available() bool {
	return c.State == pb.CollectionState_CollectionCreated
}

func (c *Collection) ShallowClone() *Collection {
	return &Collection{
		TenantID:             c.TenantID,
		DBID:                 c.DBID,
		CollectionID:         c.CollectionID,
		Name:                 c.Name,
		DBName:               c.DBName,
		Description:          c.Description,
		AutoID:               c.AutoID,
		Fields:               c.Fields,
		StructArrayFields:    c.StructArrayFields,
		Partitions:           c.Partitions,
		VirtualChannelNames:  c.VirtualChannelNames,
		PhysicalChannelNames: c.PhysicalChannelNames,
		ShardsNum:            c.ShardsNum,
		ConsistencyLevel:     c.ConsistencyLevel,
		CreateTime:           c.CreateTime,
		StartPositions:       c.StartPositions,
		Aliases:              c.Aliases,
		Properties:           c.Properties,
		State:                c.State,
		EnableDynamicField:   c.EnableDynamicField,
		EnableNamespace:      c.EnableNamespace,
		Functions:            c.Functions,
		UpdateTimestamp:      c.UpdateTimestamp,
		SchemaVersion:        c.SchemaVersion,
		ShardInfos:           c.ShardInfos,
		RoutingMode:          c.RoutingMode,
		FileResourceIds:      c.FileResourceIds,
		ExternalSource:       c.ExternalSource,
		ExternalSpec:         c.ExternalSpec,
	}
}

func (c *Collection) Clone() *Collection {
	shardInfos := make(map[string]*ShardInfo, len(c.ShardInfos))
	for channelName, shardInfo := range c.ShardInfos {
		shardInfos[channelName] = &ShardInfo{
			VChannelName:           channelName,
			PChannelName:           shardInfo.PChannelName,
			LastTruncateTimeTick:   shardInfo.LastTruncateTimeTick,
			State:                  shardInfo.State,
			SourceVChannels:        slices.Clone(shardInfo.SourceVChannels),
			FrontingSourceVChannel: shardInfo.FrontingSourceVChannel,
			Ranges:                 cloneRoutingKeyRanges(shardInfo.Ranges),
			Buckets:                slices.Clone(shardInfo.Buckets),
		}
	}
	return &Collection{
		TenantID:             c.TenantID,
		DBID:                 c.DBID,
		CollectionID:         c.CollectionID,
		Name:                 c.Name,
		DBName:               c.DBName,
		Description:          c.Description,
		AutoID:               c.AutoID,
		Fields:               CloneFields(c.Fields),
		StructArrayFields:    CloneStructArrayFields(c.StructArrayFields),
		Partitions:           ClonePartitions(c.Partitions),
		VirtualChannelNames:  common.CloneStringList(c.VirtualChannelNames),
		PhysicalChannelNames: common.CloneStringList(c.PhysicalChannelNames),
		ShardsNum:            c.ShardsNum,
		ConsistencyLevel:     c.ConsistencyLevel,
		CreateTime:           c.CreateTime,
		StartPositions:       common.CloneKeyDataPairs(c.StartPositions),
		Aliases:              common.CloneStringList(c.Aliases),
		Properties:           common.CloneKeyValuePairs(c.Properties),
		State:                c.State,
		EnableDynamicField:   c.EnableDynamicField,
		EnableNamespace:      c.EnableNamespace,
		Functions:            CloneFunctions(c.Functions),
		UpdateTimestamp:      c.UpdateTimestamp,
		SchemaVersion:        c.SchemaVersion,
		ShardInfos:           shardInfos,
		RoutingMode:          c.RoutingMode,
		FileResourceIds:      slices.Clone(c.FileResourceIds),
		ExternalSource:       c.ExternalSource,
		ExternalSpec:         c.ExternalSpec,
	}
}

// ToCollectionSchemaPB returns a schemapb.CollectionSchema populated from the
// current Collection. All schema-level fields are copied verbatim — callers
// override Version, Properties, EnableDynamicField, etc. after the call when
// the operation requires a different value.
//
// Centralizing the conversion here ensures that newly added schema fields are
// propagated consistently across every rootcoord broadcast/response path.
func (c *Collection) ToCollectionSchemaPB() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:               c.Name,
		Description:        c.Description,
		AutoID:             c.AutoID,
		Fields:             MarshalFieldModels(c.Fields),
		StructArrayFields:  MarshalStructArrayFieldModels(c.StructArrayFields),
		Functions:          MarshalFunctionModels(c.Functions),
		EnableDynamicField: c.EnableDynamicField,
		EnableNamespace:    c.EnableNamespace,
		Properties:         c.Properties,
		DbName:             c.DBName,
		Version:            c.SchemaVersion,
		FileResourceIds:    c.FileResourceIds,
		ExternalSource:     c.ExternalSource,
		ExternalSpec:       c.ExternalSpec,
	}
}

func (c *Collection) GetPartitionNum(filterUnavailable bool) int {
	if !filterUnavailable {
		return len(c.Partitions)
	}
	return lo.CountBy(c.Partitions, func(p *Partition) bool { return p.Available() })
}

func (c *Collection) Equal(other Collection) bool {
	return c.TenantID == other.TenantID &&
		c.DBID == other.DBID &&
		CheckPartitionsEqual(c.Partitions, other.Partitions) &&
		c.Name == other.Name &&
		c.Description == other.Description &&
		c.AutoID == other.AutoID &&
		CheckFieldsEqual(c.Fields, other.Fields) &&
		CheckStructArrayFieldsEqual(c.StructArrayFields, other.StructArrayFields) &&
		c.ShardsNum == other.ShardsNum &&
		c.ConsistencyLevel == other.ConsistencyLevel &&
		checkParamsEqual(c.Properties, other.Properties) &&
		c.EnableDynamicField == other.EnableDynamicField &&
		c.EnableNamespace == other.EnableNamespace
}

func (c *Collection) ApplyUpdates(header *message.AlterCollectionMessageHeader, body *message.AlterCollectionMessageBody) {
	updateMask := header.UpdateMask
	updates := body.Updates
	for _, field := range updateMask.GetPaths() {
		switch field {
		case message.FieldMaskDB:
			c.DBID = updates.DbId
			c.DBName = updates.DbName
		case message.FieldMaskCollectionName:
			c.Name = updates.CollectionName
		case message.FieldMaskCollectionDescription:
			c.Description = updates.Description
		case message.FieldMaskCollectionConsistencyLevel:
			c.ConsistencyLevel = updates.ConsistencyLevel
		case message.FieldMaskCollectionProperties:
			c.Properties = updates.Properties
		case message.FieldMaskCollectionSchema:
			c.AutoID = updates.Schema.AutoID
			c.Fields = UnmarshalFieldModels(updates.Schema.Fields)
			c.EnableDynamicField = updates.Schema.EnableDynamicField
			c.EnableNamespace = updates.Schema.EnableNamespace
			c.Functions = UnmarshalFunctionModels(updates.Schema.Functions)
			c.StructArrayFields = UnmarshalStructArrayFieldModels(updates.Schema.StructArrayFields)
			c.SchemaVersion = updates.Schema.Version
			c.FileResourceIds = updates.Schema.GetFileResourceIds()
			c.ExternalSource = updates.Schema.ExternalSource
			c.ExternalSpec = updates.Schema.ExternalSpec
		case message.FieldMaskCollectionExternalSpec:
			// Defensive: only overwrite when the update carries a value.
			// Legacy WAL messages from before the atomic-tuple invariant may
			// arrive with one half empty; preserving the existing field in
			// that case avoids silently clearing a previously persisted
			// source or spec on replay.
			if v := updates.Schema.GetExternalSource(); v != "" {
				c.ExternalSource = v
			}
			if v := updates.Schema.GetExternalSpec(); v != "" {
				c.ExternalSpec = v
			}
		case message.FieldMaskCollectionShardSplitRouting:
			// A shard split commits the whole new routing topology atomically:
			// the grown vchannel list, every shard's key range and lifecycle
			// state, and the routing mode. The channel and shard-info arrays are
			// parallel, so the ShardInfos map is rebuilt from them in lockstep.
			c.VirtualChannelNames = updates.VirtualChannelNames
			c.PhysicalChannelNames = updates.PhysicalChannelNames
			c.RoutingMode = updates.RoutingMode
			shardInfos := make(map[string]*ShardInfo, len(updates.VirtualChannelNames))
			for i, vchannel := range updates.VirtualChannelNames {
				var pchannel string
				if i < len(updates.PhysicalChannelNames) {
					pchannel = updates.PhysicalChannelNames[i]
				}
				var si *schemapb.CollectionShardInfo
				if i < len(updates.ShardInfos) {
					si = updates.ShardInfos[i]
				}
				shardInfos[vchannel] = shardInfoFromPB(vchannel, pchannel, si)
			}
			c.ShardInfos = shardInfos
			c.ShardsNum = routableShardCount(updates.ShardInfos)
		}
	}
}

func UnmarshalCollectionModel(coll *pb.CollectionInfo) *Collection {
	if coll == nil {
		return nil
	}

	// backward compatible for deprecated fields
	partitions := make([]*Partition, len(coll.PartitionIDs))
	for idx := range coll.PartitionIDs {
		partitions[idx] = &Partition{
			PartitionID:               coll.PartitionIDs[idx],
			PartitionName:             coll.PartitionNames[idx],
			PartitionCreatedTimestamp: coll.PartitionCreatedTimestamps[idx],
		}
	}
	shardInfos := make(map[string]*ShardInfo, len(coll.VirtualChannelNames))
	for idx, channelName := range coll.VirtualChannelNames {
		var si *schemapb.CollectionShardInfo
		if idx < len(coll.ShardInfos) {
			si = coll.ShardInfos[idx]
		}
		shardInfos[channelName] = shardInfoFromPB(channelName, coll.PhysicalChannelNames[idx], si)
	}

	return &Collection{
		CollectionID:         coll.ID,
		DBID:                 coll.DbId,
		Name:                 coll.Schema.Name,
		DBName:               coll.Schema.DbName,
		Description:          coll.Schema.Description,
		AutoID:               coll.Schema.AutoID,
		Fields:               UnmarshalFieldModels(coll.GetSchema().GetFields()),
		StructArrayFields:    UnmarshalStructArrayFieldModels(coll.GetSchema().GetStructArrayFields()),
		Partitions:           partitions,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		CreateTime:           coll.CreateTime,
		StartPositions:       coll.StartPositions,
		State:                coll.State,
		Properties:           coll.Properties,
		EnableDynamicField:   coll.Schema.EnableDynamicField,
		EnableNamespace:      coll.Schema.EnableNamespace,
		UpdateTimestamp:      coll.UpdateTimestamp,
		SchemaVersion:        coll.Schema.Version,
		ShardInfos:           shardInfos,
		RoutingMode:          coll.RoutingMode,
		FileResourceIds:      coll.Schema.GetFileResourceIds(),
		ExternalSource:       coll.Schema.ExternalSource,
		ExternalSpec:         coll.Schema.ExternalSpec,
	}
}

// MarshalCollectionModel marshal only collection-related information.
// partitions, aliases and fields won't be marshaled. They should be written to newly path.
func MarshalCollectionModel(coll *Collection) *pb.CollectionInfo {
	return marshalCollectionModelWithConfig(coll, newDefaultConfig())
}

type config struct {
	withFields            bool
	withPartitions        bool
	withStructArrayFields bool
}

type Option func(c *config)

func newDefaultConfig() *config {
	return &config{withFields: false, withPartitions: false, withStructArrayFields: false}
}

func WithFields() Option {
	return func(c *config) {
		c.withFields = true
	}
}

func WithPartitions() Option {
	return func(c *config) {
		c.withPartitions = true
	}
}

func WithStructArrayFields() Option {
	return func(c *config) {
		c.withStructArrayFields = true
	}
}

func marshalCollectionModelWithConfig(coll *Collection, c *config) *pb.CollectionInfo {
	if coll == nil {
		return nil
	}

	collSchema := &schemapb.CollectionSchema{
		Name:               coll.Name,
		Description:        coll.Description,
		AutoID:             coll.AutoID,
		EnableDynamicField: coll.EnableDynamicField,
		EnableNamespace:    coll.EnableNamespace,
		DbName:             coll.DBName,
		Version:            coll.SchemaVersion,
		FileResourceIds:    coll.FileResourceIds,
		ExternalSource:     coll.ExternalSource,
		ExternalSpec:       coll.ExternalSpec,
	}

	if c.withFields {
		fields := MarshalFieldModels(coll.Fields)
		collSchema.Fields = fields
	}

	if c.withStructArrayFields {
		structArrayFields := MarshalStructArrayFieldModels(coll.StructArrayFields)
		collSchema.StructArrayFields = structArrayFields
	}

	// size by the index domain (vchannel positions), not the map length: the
	// loop below indexes shardInfos[idx] by VirtualChannelNames position.
	shardInfos := make([]*schemapb.CollectionShardInfo, len(coll.VirtualChannelNames))
	for idx, channelName := range coll.VirtualChannelNames {
		if shard, ok := coll.ShardInfos[channelName]; ok {
			shardInfos[idx] = shard.ToPB()
		} else {
			shardInfos[idx] = &schemapb.CollectionShardInfo{}
		}
	}
	collectionPb := &pb.CollectionInfo{
		ID:                   coll.CollectionID,
		DbId:                 coll.DBID,
		Schema:               collSchema,
		CreateTime:           coll.CreateTime,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		StartPositions:       coll.StartPositions,
		State:                coll.State,
		Properties:           coll.Properties,
		UpdateTimestamp:      coll.UpdateTimestamp,
		ShardInfos:           shardInfos,
		RoutingMode:          coll.RoutingMode,
	}

	if c.withPartitions {
		for _, partition := range coll.Partitions {
			collectionPb.PartitionNames = append(collectionPb.PartitionNames, partition.PartitionName)
			collectionPb.PartitionIDs = append(collectionPb.PartitionIDs, partition.PartitionID)
			collectionPb.PartitionCreatedTimestamps = append(collectionPb.PartitionCreatedTimestamps, partition.PartitionCreatedTimestamp)
		}
	}

	return collectionPb
}

func MarshalCollectionModelWithOption(coll *Collection, opts ...Option) *pb.CollectionInfo {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return marshalCollectionModelWithConfig(coll, c)
}
