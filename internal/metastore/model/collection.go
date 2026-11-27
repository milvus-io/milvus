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
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// TODO: These collection is dirty implementation and easy to be broken, we should drop it in the future.
//
// Fields and the Fields of each StructArrayFields entry are shared with
// the published collection view (see SchemaView) and with every
// ShallowClone: they are read-only. To change a field, replace it with
// a modified CloneField, or modify a Clone of the collection.
type Collection struct {
	TenantID          string
	DBID              int64
	CollectionID      int64
	Partitions        []*Partition
	Name              string
	DBName            string
	Description       string
	AutoID            bool
	Fields            []*Field
	StructArrayFields []*StructArrayField
	Functions         []*Function

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
	// RoutingModulus is the modulus the residues of the shards are taken
	// against. Zero on a collection that was never split; it routes by
	// hash % ShardsNum.
	RoutingModulus uint64
	// ShardBy is the routing key expression, "hash(<field>)". Empty if it was
	// never declared.
	ShardBy string
	// SplitLineages has one record for every shard of the collection that was
	// split. A split shard is not in VirtualChannelNames, PhysicalChannelNames
	// or ShardInfos: it accepts no DML and receives no DDL broadcast.
	SplitLineages   []*ShardSplitLineage
	FileResourceIds []int64
	ExternalSource  string
	ExternalSpec    string
}

type ShardInfo struct {
	PChannelName         string // the pchannel name of the shard, it is the same with the physical channel name.
	VChannelName         string // the vchannel name of the shard, it is the same with the virtual channel name.
	LastTruncateTimeTick uint64 // the last truncate time tick of the shard, if the shard is not truncated, the value is 0.
	// Buckets are the residues the shard owns, taken against the
	// RoutingModulus of the collection. Empty on a collection that was never
	// split.
	Buckets []uint64
}

// ShardSplitLineage records that a shard was split. It is written once, when
// the SplitShard message is applied, and never changed.
type ShardSplitLineage struct {
	SourceVChannel  string
	SourcePChannel  string
	TargetVChannels []string
	// SwitchTimeTick is the time tick of the SplitShard message on the source
	// vchannel, in the WAL of this cluster.
	SwitchTimeTick uint64
	SplitTaskID    int64
}

func (l *ShardSplitLineage) clone() *ShardSplitLineage {
	return &ShardSplitLineage{
		SourceVChannel:  l.SourceVChannel,
		SourcePChannel:  l.SourcePChannel,
		TargetVChannels: slices.Clone(l.TargetVChannels),
		SwitchTimeTick:  l.SwitchTimeTick,
		SplitTaskID:     l.SplitTaskID,
	}
}

func cloneSplitLineages(lineages []*ShardSplitLineage) []*ShardSplitLineage {
	if lineages == nil {
		return nil
	}
	cloned := make([]*ShardSplitLineage, 0, len(lineages))
	for _, lineage := range lineages {
		cloned = append(cloned, lineage.clone())
	}
	return cloned
}

// Routing returns the routing of the collection: the shards that accept DML,
// in the order of VirtualChannelNames, with the residues they own.
func (c *Collection) Routing() routing.Routing {
	r := routing.Routing{
		Modulus: c.RoutingModulus,
		ShardBy: c.ShardBy,
		Shards:  make([]routing.Shard, 0, len(c.VirtualChannelNames)),
	}
	for _, vchannel := range c.VirtualChannelNames {
		shard := routing.Shard{VChannel: vchannel}
		if info, ok := c.ShardInfos[vchannel]; ok {
			shard.Buckets = slices.Clone(info.Buckets)
		}
		r.Shards = append(r.Shards, shard)
	}
	return r
}

// SplitLineageOf returns the lineage record of a shard that was split, or nil.
func (c *Collection) SplitLineageOf(vchannel string) *ShardSplitLineage {
	for _, lineage := range c.SplitLineages {
		if lineage.SourceVChannel == vchannel {
			return lineage
		}
	}
	return nil
}

// SplitSourceVChannels returns the vchannels of the collection that were split.
func (c *Collection) SplitSourceVChannels() []string {
	vchannels := make([]string, 0, len(c.SplitLineages))
	for _, lineage := range c.SplitLineages {
		vchannels = append(vchannels, lineage.SourceVChannel)
	}
	return vchannels
}

// SplitSourceBuckets returns the residues, at the current RoutingModulus, of
// the keys a split source owned before it was split. They are the residues of
// the shards that own its keys now: its targets, or, for a target that was
// split in turn, the targets of that target. The modulus only grows by
// doubling and a split moves no key of another shard, so the union is exact.
//
// It returns nil if vchannel is not a split source, or if a shard on the way
// is neither a shard of the routing nor a split source (malformed metadata).
func (c *Collection) SplitSourceBuckets(vchannel string) []uint64 {
	if c.RoutingModulus == 0 || c.SplitLineageOf(vchannel) == nil {
		return nil
	}
	var buckets []uint64
	visited := make(map[string]struct{})
	var collect func(vchannel string) bool
	collect = func(vchannel string) bool {
		if info, ok := c.ShardInfos[vchannel]; ok {
			buckets = append(buckets, info.Buckets...)
			return len(info.Buckets) > 0
		}
		lineage := c.SplitLineageOf(vchannel)
		if _, seen := visited[vchannel]; seen || lineage == nil {
			return false
		}
		visited[vchannel] = struct{}{}
		for _, target := range lineage.TargetVChannels {
			if !collect(target) {
				return false
			}
		}
		return len(lineage.TargetVChannels) > 0
	}
	if !collect(vchannel) {
		return nil
	}
	slices.Sort(buckets)
	return slices.Compact(buckets)
}

// ApplyShardSplit replaces the routing of the collection by the routing after
// a shard split and records the lineage of the shard that was split.
//
// The source leaves VirtualChannelNames, PhysicalChannelNames and ShardInfos.
// A shard that exists before and after keeps its last truncate time tick; a
// new shard starts at zero. The caller validates next and lineage.
func (c *Collection) ApplyShardSplit(next routing.Routing, lineage *ShardSplitLineage) {
	vchannels := next.VChannels()
	pchannels := make([]string, 0, len(vchannels))
	shardInfos := make(map[string]*ShardInfo, len(vchannels))
	for _, shard := range next.Shards {
		pchannel := funcutil.ToPhysicalChannel(shard.VChannel)
		info := &ShardInfo{
			VChannelName: shard.VChannel,
			PChannelName: pchannel,
			Buckets:      slices.Clone(shard.Buckets),
		}
		if old, ok := c.ShardInfos[shard.VChannel]; ok {
			info.PChannelName = old.PChannelName
			info.LastTruncateTimeTick = old.LastTruncateTimeTick
		}
		pchannels = append(pchannels, info.PChannelName)
		shardInfos[shard.VChannel] = info
	}
	c.VirtualChannelNames = vchannels
	c.PhysicalChannelNames = pchannels
	c.ShardInfos = shardInfos
	c.ShardsNum = int32(len(vchannels))
	c.RoutingModulus = next.Modulus
	c.ShardBy = next.ShardBy
	c.SplitLineages = append(cloneSplitLineages(c.SplitLineages), lineage.clone())
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
		RoutingModulus:       c.RoutingModulus,
		ShardBy:              c.ShardBy,
		SplitLineages:        c.SplitLineages,
		FileResourceIds:      c.FileResourceIds,
		ExternalSource:       c.ExternalSource,
		ExternalSpec:         c.ExternalSpec,
	}
}

func (c *Collection) Clone() *Collection {
	shardInfos := make(map[string]*ShardInfo, len(c.ShardInfos))
	for channelName, shardInfo := range c.ShardInfos {
		shardInfos[channelName] = &ShardInfo{
			VChannelName:         channelName,
			PChannelName:         shardInfo.PChannelName,
			LastTruncateTimeTick: shardInfo.LastTruncateTimeTick,
			Buckets:              slices.Clone(shardInfo.Buckets),
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
		RoutingModulus:       c.RoutingModulus,
		ShardBy:              c.ShardBy,
		SplitLineages:        cloneSplitLineages(c.SplitLineages),
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
	return c.schemaPB(CloneFields)
}

// SchemaView returns the schema ToCollectionSchemaPB returns, except that
// it shares the model's field schemas instead of copying them: Fields
// holds c.Fields' elements, and each StructArrayFields entry is a new
// shell whose Fields holds the model's sub-fields. Callers must not
// modify the fields; use ToCollectionSchemaPB for a schema they may
// change.
func (c *Collection) SchemaView() *schemapb.CollectionSchema {
	// Clip so an append by a reader reallocates instead of writing into
	// the model's backing array.
	return c.schemaPB(slices.Clip[[]*Field])
}

// schemaPB builds the CollectionSchema with Fields and each struct
// field's sub-fields taken from fields(...).
func (c *Collection) schemaPB(fields func([]*Field) []*Field) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:               c.Name,
		Description:        c.Description,
		AutoID:             c.AutoID,
		Fields:             fields(c.Fields),
		StructArrayFields:  marshalStructArrayFieldModels(c.StructArrayFields, fields),
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
			c.Fields = CloneFields(updates.Schema.Fields)
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
		info := &ShardInfo{
			VChannelName: channelName,
			PChannelName: coll.PhysicalChannelNames[idx],
		}
		// A record written before shard infos existed has none.
		if idx < len(coll.ShardInfos) {
			info.LastTruncateTimeTick = coll.ShardInfos[idx].GetLastTruncateTimeTick()
			info.Buckets = slices.Clone(coll.ShardInfos[idx].GetHashRouting().GetBuckets())
		}
		shardInfos[channelName] = info
	}
	var splitLineages []*ShardSplitLineage
	for _, lineage := range coll.GetSplitLineages() {
		splitLineages = append(splitLineages, &ShardSplitLineage{
			SourceVChannel:  lineage.GetSourceVchannel(),
			SourcePChannel:  lineage.GetSourcePchannel(),
			TargetVChannels: slices.Clone(lineage.GetTargetVchannels()),
			SwitchTimeTick:  lineage.GetSwitchTimeTick(),
			SplitTaskID:     lineage.GetSplitTaskId(),
		})
	}

	return &Collection{
		CollectionID:         coll.ID,
		DBID:                 coll.DbId,
		Name:                 coll.Schema.Name,
		DBName:               coll.Schema.DbName,
		Description:          coll.Schema.Description,
		AutoID:               coll.Schema.AutoID,
		Fields:               CloneFields(coll.GetSchema().GetFields()),
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
		RoutingModulus:       coll.GetRoutingModulus(),
		ShardBy:              coll.GetShardBy(),
		SplitLineages:        splitLineages,
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
		fields := CloneFields(coll.Fields)
		collSchema.Fields = fields
	}

	if c.withStructArrayFields {
		structArrayFields := MarshalStructArrayFieldModels(coll.StructArrayFields)
		collSchema.StructArrayFields = structArrayFields
	}

	// Sized by the vchannel list, which is what the loop indexes by.
	shardInfos := make([]*schemapb.CollectionShardInfo, len(coll.VirtualChannelNames))
	for idx, channelName := range coll.VirtualChannelNames {
		info := &schemapb.CollectionShardInfo{}
		if shard, ok := coll.ShardInfos[channelName]; ok {
			info.LastTruncateTimeTick = shard.LastTruncateTimeTick
			// A collection that was never split stores no name and no residues,
			// so its record is byte-identical to one written before shard split.
			if len(shard.Buckets) > 0 {
				info.VchannelName = channelName
				info.Routing = &schemapb.CollectionShardInfo_HashRouting{
					HashRouting: &schemapb.HashRouting{Buckets: slices.Clone(shard.Buckets)},
				}
			}
		}
		shardInfos[idx] = info
	}
	var splitLineages []*pb.ShardSplitLineage
	for _, lineage := range coll.SplitLineages {
		splitLineages = append(splitLineages, &pb.ShardSplitLineage{
			SourceVchannel:  lineage.SourceVChannel,
			SourcePchannel:  lineage.SourcePChannel,
			TargetVchannels: slices.Clone(lineage.TargetVChannels),
			SwitchTimeTick:  lineage.SwitchTimeTick,
			SplitTaskId:     lineage.SplitTaskID,
		})
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
		RoutingModulus:       coll.RoutingModulus,
		ShardBy:              coll.ShardBy,
		SplitLineages:        splitLineages,
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
