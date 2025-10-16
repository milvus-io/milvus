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

package rootcoord

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

type stepPriority int

const (
	stepPriorityLow       = 0
	stepPriorityNormal    = 1
	stepPriorityImportant = 10
	stepPriorityUrgent    = 1000
)

type nestedStep interface {
	Execute(ctx context.Context) ([]nestedStep, error)
	Desc() string
	Weight() stepPriority
}

type baseStep struct {
	core *Core
}

func (s baseStep) Desc() string {
	return ""
}

func (s baseStep) Weight() stepPriority {
	return stepPriorityLow
}

type cleanupMetricsStep struct {
	baseStep
	dbName         string
	collectionName string
}

func (s *cleanupMetricsStep) Execute(ctx context.Context) ([]nestedStep, error) {
	metrics.CleanupRootCoordCollectionMetrics(s.dbName, s.collectionName)
	return nil, nil
}

func (s *cleanupMetricsStep) Desc() string {
	return fmt.Sprintf("change collection state, db: %s, collectionstate: %s",
		s.dbName, s.collectionName)
}

type expireCacheStep struct {
	baseStep
	dbName          string
	collectionNames []string
	collectionID    UniqueID
	partitionName   string
	ts              Timestamp
	opts            []proxyutil.ExpireCacheOpt
}

func (s *expireCacheStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.ExpireMetaCache(ctx, s.dbName, s.collectionNames, s.collectionID, s.partitionName, s.ts, s.opts...)
	return nil, err
}

func (s *expireCacheStep) Desc() string {
	return fmt.Sprintf("expire cache, collection id: %d, collection names: %s, ts: %d",
		s.collectionID, s.collectionNames, s.ts)
}

type releaseCollectionStep struct {
	baseStep
	collectionID UniqueID
}

func (s *releaseCollectionStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.broker.ReleaseCollection(ctx, s.collectionID)
	log.Ctx(ctx).Info("release collection done", zap.Int64("collectionID", s.collectionID))
	return nil, err
}

func (s *releaseCollectionStep) Desc() string {
	return fmt.Sprintf("release collection: %d", s.collectionID)
}

func (s *releaseCollectionStep) Weight() stepPriority {
	return stepPriorityUrgent
}

type releasePartitionsStep struct {
	baseStep
	collectionID UniqueID
	partitionIDs []UniqueID
}

func (s *releasePartitionsStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.broker.ReleasePartitions(ctx, s.collectionID, s.partitionIDs...)
	return nil, err
}

func (s *releasePartitionsStep) Desc() string {
	return fmt.Sprintf("release partitions, collectionID=%d, partitionIDs=%v", s.collectionID, s.partitionIDs)
}

func (s *releasePartitionsStep) Weight() stepPriority {
	return stepPriorityUrgent
}

type dropIndexStep struct {
	baseStep
	collID  UniqueID
	partIDs []UniqueID
}

func (s *dropIndexStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.broker.DropCollectionIndex(ctx, s.collID, s.partIDs)
	return nil, err
}

func (s *dropIndexStep) Desc() string {
	return fmt.Sprintf("drop collection index: %d", s.collID)
}

func (s *dropIndexStep) Weight() stepPriority {
	return stepPriorityNormal
}

type nullStep struct{}

func (s *nullStep) Execute(ctx context.Context) ([]nestedStep, error) {
	return nil, nil
}

func (s *nullStep) Desc() string {
	return ""
}

func (s *nullStep) Weight() stepPriority {
	return stepPriorityLow
}

type AlterCollectionStep struct {
	baseStep
	oldColl     *model.Collection
	newColl     *model.Collection
	ts          Timestamp
	fieldModify bool
}

func (a *AlterCollectionStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := a.core.meta.AlterCollection(ctx, a.oldColl, a.newColl, a.ts, a.fieldModify)
	return nil, err
}

func (a *AlterCollectionStep) Desc() string {
	return fmt.Sprintf("alter collection, collectionID: %d, ts: %d", a.oldColl.CollectionID, a.ts)
}

type BroadcastAlteredCollectionStep struct {
	baseStep
	req  *milvuspb.AlterCollectionRequest
	core *Core
}

func (b *BroadcastAlteredCollectionStep) Execute(ctx context.Context) ([]nestedStep, error) {
	// TODO: support online schema change mechanism
	// It only broadcast collection properties to DataCoord service
	err := b.core.broker.BroadcastAlteredCollection(ctx, b.req)
	return nil, err
}

func (b *BroadcastAlteredCollectionStep) Desc() string {
	return fmt.Sprintf("broadcast altered collection, collectionID: %d", b.req.CollectionID)
}

type AddCollectionFieldStep struct {
	baseStep
	oldColl           *model.Collection
	updatedCollection *model.Collection
	newField          *model.Field
	ts                Timestamp
}

func (a *AddCollectionFieldStep) Execute(ctx context.Context) ([]nestedStep, error) {
	// newColl := a.oldColl.Clone()
	// newColl.Fields = append(newColl.Fields, a.newField)
	err := a.core.meta.AlterCollection(ctx, a.oldColl, a.updatedCollection, a.updatedCollection.UpdateTimestamp, true)
	log.Ctx(ctx).Info("add field done", zap.Int64("collectionID", a.oldColl.CollectionID), zap.Any("new field", a.newField))
	return nil, err
}

func (a *AddCollectionFieldStep) Desc() string {
	return fmt.Sprintf("add field, collectionID: %d, fieldID: %d, ts: %d", a.oldColl.CollectionID, a.newField.FieldID, a.ts)
}

type WriteSchemaChangeWALStep struct {
	baseStep
	collection *model.Collection
	ts         Timestamp
}

func (s *WriteSchemaChangeWALStep) Execute(ctx context.Context) ([]nestedStep, error) {
	vchannels := s.collection.VirtualChannelNames
	schema := &schemapb.CollectionSchema{
		Name:               s.collection.Name,
		Description:        s.collection.Description,
		AutoID:             s.collection.AutoID,
		Fields:             model.MarshalFieldModels(s.collection.Fields),
		StructArrayFields:  model.MarshalStructArrayFieldModels(s.collection.StructArrayFields),
		Functions:          model.MarshalFunctionModels(s.collection.Functions),
		EnableDynamicField: s.collection.EnableDynamicField,
		Properties:         s.collection.Properties,
	}

	schemaMsg, err := message.NewSchemaChangeMessageBuilderV2().
		WithBroadcast(vchannels).
		WithHeader(&message.SchemaChangeMessageHeader{
			CollectionId: s.collection.CollectionID,
		}).
		WithBody(&message.SchemaChangeMessageBody{
			Schema: schema,
		}).BuildBroadcast()
	if err != nil {
		return nil, err
	}

	resp, err := streaming.WAL().Broadcast().Append(ctx, schemaMsg)
	if err != nil {
		return nil, err
	}

	// use broadcast max msg timestamp as update timestamp here
	s.collection.UpdateTimestamp = lo.Max(lo.Map(vchannels, func(channelName string, _ int) uint64 {
		return resp.GetAppendResult(channelName).TimeTick
	}))
	log.Ctx(ctx).Info(
		"broadcast schema change success",
		zap.Uint64("broadcastID", resp.BroadcastID),
		zap.Uint64("WALUpdateTimestamp", s.collection.UpdateTimestamp),
		zap.Any("appendResults", resp.AppendResults),
	)
	return nil, nil
}

func (s *WriteSchemaChangeWALStep) Desc() string {
	return fmt.Sprintf("write schema change WALcollectionID: %d, ts: %d", s.collection.CollectionID, s.ts)
}

type renameCollectionStep struct {
	baseStep
	dbName    string
	oldName   string
	newDBName string
	newName   string
	ts        Timestamp
}

func (s *renameCollectionStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.meta.RenameCollection(ctx, s.dbName, s.oldName, s.newDBName, s.newName, s.ts)
	return nil, err
}

func (s *renameCollectionStep) Desc() string {
	return fmt.Sprintf("rename collection from %s.%s to %s.%s, ts: %d",
		s.dbName, s.oldName, s.newDBName, s.newName, s.ts)
}

var (
	confirmGCInterval          = time.Minute * 20
	allPartition      UniqueID = common.AllPartitionsID
)

type confirmGCStep struct {
	baseStep
	collectionID      UniqueID
	partitionID       UniqueID
	lastScheduledTime time.Time
}

func newConfirmGCStep(core *Core, collectionID, partitionID UniqueID) *confirmGCStep {
	return &confirmGCStep{
		baseStep:          baseStep{core: core},
		collectionID:      collectionID,
		partitionID:       partitionID,
		lastScheduledTime: time.Now(),
	}
}

func (b *confirmGCStep) Execute(ctx context.Context) ([]nestedStep, error) {
	if time.Since(b.lastScheduledTime) < confirmGCInterval {
		return nil, fmt.Errorf("wait for reschedule to confirm GC, collection: %d, partition: %d, last scheduled time: %s, now: %s",
			b.collectionID, b.partitionID, b.lastScheduledTime.String(), time.Now().String())
	}

	finished := b.core.broker.GcConfirm(ctx, b.collectionID, b.partitionID)
	if finished {
		return nil, nil
	}

	b.lastScheduledTime = time.Now()

	return nil, fmt.Errorf("GC is not finished, collection: %d, partition: %d, last scheduled time: %s, now: %s",
		b.collectionID, b.partitionID, b.lastScheduledTime.String(), time.Now().String())
}

func (b *confirmGCStep) Desc() string {
	return fmt.Sprintf("wait for GC finished, collection: %d, partition: %d, last scheduled time: %s, now: %s",
		b.collectionID, b.partitionID, b.lastScheduledTime.String(), time.Now().String())
}

func (b *confirmGCStep) Weight() stepPriority {
	return stepPriorityLow
}

type simpleStep struct {
	desc        string
	weight      stepPriority
	executeFunc func(ctx context.Context) ([]nestedStep, error)
}

func NewSimpleStep(desc string, executeFunc func(ctx context.Context) ([]nestedStep, error)) nestedStep {
	return &simpleStep{
		desc:        desc,
		weight:      stepPriorityNormal,
		executeFunc: executeFunc,
	}
}

func (s *simpleStep) Execute(ctx context.Context) ([]nestedStep, error) {
	return s.executeFunc(ctx)
}

func (s *simpleStep) Desc() string {
	return s.desc
}

func (s *simpleStep) Weight() stepPriority {
	return s.weight
}
