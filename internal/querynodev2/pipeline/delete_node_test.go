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

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type DeleteNodeSuite struct {
	suite.Suite
	// datas
	collectionID   int64
	collectionName string
	partitionIDs   []int64
	deletePKs      []int64
	channel        string
	timeRange      TimeRange

	// mocks
	manager   *segments.Manager
	delegator *delegator.MockShardDelegator
}

func (suite *DeleteNodeSuite) SetupSuite() {
	paramtable.Init()
	suite.collectionID = 111
	suite.collectionName = "test-collection"
	suite.partitionIDs = []int64{11, 22}
	suite.channel = "test-channel"
	// segment own data row which‘s pk same with segment‘s ID
	suite.deletePKs = []int64{1, 2, 3, 4}
	suite.timeRange = TimeRange{
		timestampMin: 0,
		timestampMax: 1,
	}
}

func (suite *DeleteNodeSuite) buildDeleteNodeMsg() *deleteNodeMsg {
	nodeMsg := &deleteNodeMsg{
		deleteMsgs: []*DeleteMsg{},
		timeRange:  suite.timeRange,
	}

	for i, pk := range suite.deletePKs {
		deleteMsg := buildDeleteMsg(suite.collectionID, suite.partitionIDs[i%len(suite.partitionIDs)], suite.channel, 1)
		deleteMsg.PrimaryKeys = genDeletePK(pk)
		nodeMsg.deleteMsgs = append(nodeMsg.deleteMsgs, deleteMsg)
	}
	return nodeMsg
}

func (suite *DeleteNodeSuite) TestBasic() {
	// mock
	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockSegmentManager := segments.NewMockSegmentManager(suite.T())
	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    mockSegmentManager,
	}
	suite.delegator = delegator.NewMockShardDelegator(suite.T())
	suite.delegator.EXPECT().ProcessDeleteBatches(mock.Anything).Run(
		func(batches []delegator.DeleteBatch) {
			for _, data := range batches[0].Data {
				for _, pk := range data.PrimaryKeys {
					suite.True(lo.Contains(suite.deletePKs, pk.GetValue().(int64)))
				}
			}
		})
	// init dependency
	// build delete node and data
	node := newDeleteNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, 8)
	in := suite.buildDeleteNodeMsg()
	suite.delegator.EXPECT().UpdateTSafe(in.timeRange.timestampMax).Return()
	// run
	out := node.Operate(in)
	suite.Nil(out)
}

func (suite *DeleteNodeSuite) TestProcessDeleteBatchesUseDeleteMsgEndTs() {
	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockSegmentManager := segments.NewMockSegmentManager(suite.T())
	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    mockSegmentManager,
	}
	suite.delegator = delegator.NewMockShardDelegator(suite.T())

	first := buildDeleteMsg(suite.collectionID, suite.partitionIDs[0], suite.channel, 1)
	first.SetTs(10)
	first.PrimaryKeys = genDeletePK(10)
	second := buildDeleteMsg(suite.collectionID, suite.partitionIDs[1], suite.channel, 1)
	second.SetTs(20)
	second.PrimaryKeys = genDeletePK(20)
	third := buildDeleteMsg(suite.collectionID, suite.partitionIDs[0], suite.channel, 1)
	third.SetTs(10)
	third.PrimaryKeys = genDeletePK(30)

	in := &deleteNodeMsg{
		deleteMsgs: []*DeleteMsg{first, second, third},
		timeRange: TimeRange{
			timestampMin: 10,
			timestampMax: 30,
		},
	}

	suite.delegator.EXPECT().ProcessDeleteBatches(mock.Anything).Run(
		func(batches []delegator.DeleteBatch) {
			suite.Require().Len(batches, 2)
			suite.Equal(uint64(10), batches[0].Ts)
			suite.Equal(uint64(20), batches[1].Ts)
			suite.Len(batches[0].Data, 1)
			suite.Len(batches[1].Data, 1)
			suite.ElementsMatch([]int64{10, 30}, lo.Map(batches[0].Data[0].PrimaryKeys, func(pk storage.PrimaryKey, _ int) int64 {
				return pk.GetValue().(int64)
			}))
			suite.ElementsMatch([]int64{20}, lo.Map(batches[1].Data[0].PrimaryKeys, func(pk storage.PrimaryKey, _ int) int64 {
				return pk.GetValue().(int64)
			}))
		})
	suite.delegator.EXPECT().UpdateTSafe(uint64(30)).Return()

	node := newDeleteNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, 8)
	out := node.Operate(in)
	suite.Nil(out)
}

func (suite *DeleteNodeSuite) TestUpdateSchemaErrorDoesNotAdvanceTSafe() {
	manager := &segments.Manager{
		Collection: segments.NewMockCollectionManager(suite.T()),
		Segment:    segments.NewMockSegmentManager(suite.T()),
	}
	delegator := delegator.NewMockShardDelegator(suite.T())
	schema := &schemapb.CollectionSchema{Version: 2}
	updateSchemaStarted := make(chan struct{})
	delegator.EXPECT().UpdateSchema(mock.Anything, schema, uint64(10)).RunAndReturn(func(ctx context.Context, _ *schemapb.CollectionSchema, _ uint64) error {
		close(updateSchemaStarted)
		<-ctx.Done()
		return ctx.Err()
	}).Once()

	node := newDeleteNode(suite.collectionID, suite.channel, manager, delegator, 8)

	done := make(chan struct{})
	go func() {
		defer close(done)
		suite.Nil(node.Operate(&deleteNodeMsg{
			schema:          schema,
			schemaBarrierTs: 10,
			timeRange:       TimeRange{timestampMax: 10},
		}))
	}()

	select {
	case <-updateSchemaStarted:
	case <-time.After(time.Second):
		suite.FailNow("schema update did not start")
	}
	node.Close()
	suite.Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func (suite *DeleteNodeSuite) TestUpdateSchemaRetriesBeforeTSafe() {
	manager := &segments.Manager{
		Collection: segments.NewMockCollectionManager(suite.T()),
		Segment:    segments.NewMockSegmentManager(suite.T()),
	}
	delegator := delegator.NewMockShardDelegator(suite.T())
	schema := &schemapb.CollectionSchema{Version: 2}
	expectedErr := merr.WrapErrServiceInternal("transient worker fanout failure")
	delegator.EXPECT().UpdateSchema(mock.Anything, schema, uint64(10)).Return(expectedErr).Once()
	delegator.EXPECT().UpdateSchema(mock.Anything, schema, uint64(10)).Return(nil).Once()
	delegator.EXPECT().UpdateTSafe(uint64(10)).Return().Once()

	node := newDeleteNode(suite.collectionID, suite.channel, manager, delegator, 8)
	oldInterval := deleteNodeUpdateSchemaRetryInterval
	deleteNodeUpdateSchemaRetryInterval = time.Millisecond
	suite.T().Cleanup(func() {
		deleteNodeUpdateSchemaRetryInterval = oldInterval
	})

	suite.Nil(node.Operate(&deleteNodeMsg{
		schema:          schema,
		schemaBarrierTs: 10,
		timeRange:       TimeRange{timestampMax: 10},
	}))
}

func (suite *DeleteNodeSuite) TestUpdateSchemaRetriesRawTransportError() {
	manager := &segments.Manager{
		Collection: segments.NewMockCollectionManager(suite.T()),
		Segment:    segments.NewMockSegmentManager(suite.T()),
	}
	delegator := delegator.NewMockShardDelegator(suite.T())
	schema := &schemapb.CollectionSchema{Version: 2}
	delegator.EXPECT().UpdateSchema(mock.Anything, schema, uint64(10)).Return(grpcstatus.Error(codes.Unavailable, "transport unavailable")).Once()
	delegator.EXPECT().UpdateSchema(mock.Anything, schema, uint64(10)).Return(nil).Once()
	delegator.EXPECT().UpdateTSafe(uint64(10)).Return().Once()

	node := newDeleteNode(suite.collectionID, suite.channel, manager, delegator, 8)
	oldInterval := deleteNodeUpdateSchemaRetryInterval
	deleteNodeUpdateSchemaRetryInterval = time.Millisecond
	suite.T().Cleanup(func() {
		deleteNodeUpdateSchemaRetryInterval = oldInterval
	})

	suite.Nil(node.Operate(&deleteNodeMsg{
		schema:          schema,
		schemaBarrierTs: 10,
		timeRange:       TimeRange{timestampMax: 10},
	}))
}

func (suite *DeleteNodeSuite) TestUpdateSchemaRetriesNodeNotAvailable() {
	manager := &segments.Manager{
		Collection: segments.NewMockCollectionManager(suite.T()),
		Segment:    segments.NewMockSegmentManager(suite.T()),
	}
	delegator := delegator.NewMockShardDelegator(suite.T())
	schema := &schemapb.CollectionSchema{Version: 2}
	delegator.EXPECT().UpdateSchema(mock.Anything, schema, uint64(10)).Return(merr.WrapErrNodeNotAvailable(1)).Once()
	delegator.EXPECT().UpdateSchema(mock.Anything, schema, uint64(10)).Return(nil).Once()
	delegator.EXPECT().UpdateTSafe(uint64(10)).Return().Once()

	node := newDeleteNode(suite.collectionID, suite.channel, manager, delegator, 8)
	oldInterval := deleteNodeUpdateSchemaRetryInterval
	deleteNodeUpdateSchemaRetryInterval = time.Millisecond
	suite.T().Cleanup(func() {
		deleteNodeUpdateSchemaRetryInterval = oldInterval
	})

	suite.Nil(node.Operate(&deleteNodeMsg{
		schema:          schema,
		schemaBarrierTs: 10,
		timeRange:       TimeRange{timestampMax: 10},
	}))
}

func (suite *DeleteNodeSuite) TestUpdateSchemaNonRetryableErrorPanicsWithoutTSafe() {
	manager := &segments.Manager{
		Collection: segments.NewMockCollectionManager(suite.T()),
		Segment:    segments.NewMockSegmentManager(suite.T()),
	}
	delegator := delegator.NewMockShardDelegator(suite.T())
	schema := &schemapb.CollectionSchema{Version: 2}
	expectedErr := merr.WrapErrOperationNotSupportedMsg("unsupported incompatible schema change")
	delegator.EXPECT().UpdateSchema(mock.Anything, schema, uint64(10)).Return(expectedErr).Once()

	node := newDeleteNode(suite.collectionID, suite.channel, manager, delegator, 8)
	suite.Panics(func() {
		node.Operate(&deleteNodeMsg{
			schema:          schema,
			schemaBarrierTs: 10,
			timeRange:       TimeRange{timestampMax: 10},
		})
	})
}

func (suite *DeleteNodeSuite) TestUpdateSchemaRetryLimitPanicsWithoutTSafe() {
	manager := &segments.Manager{
		Collection: segments.NewMockCollectionManager(suite.T()),
		Segment:    segments.NewMockSegmentManager(suite.T()),
	}
	delegator := delegator.NewMockShardDelegator(suite.T())
	schema := &schemapb.CollectionSchema{Version: 2}
	expectedErr := merr.WrapErrNodeNotAvailable(1)
	delegator.EXPECT().UpdateSchema(mock.Anything, schema, uint64(10)).Return(expectedErr).Once()

	node := newDeleteNode(suite.collectionID, suite.channel, manager, delegator, 8)
	oldMaxRetryDuration := deleteNodeUpdateSchemaMaxRetryDuration
	deleteNodeUpdateSchemaMaxRetryDuration = 0
	suite.T().Cleanup(func() {
		deleteNodeUpdateSchemaMaxRetryDuration = oldMaxRetryDuration
	})

	suite.Panics(func() {
		node.Operate(&deleteNodeMsg{
			schema:          schema,
			schemaBarrierTs: 10,
			timeRange:       TimeRange{timestampMax: 10},
		})
	})
}

func TestDeleteNode(t *testing.T) {
	suite.Run(t, new(DeleteNodeSuite))
}

func TestSchemaUpdateRetryClassification(t *testing.T) {
	assert.True(t, isRetryableSchemaUpdateError(grpcstatus.Error(codes.FailedPrecondition, "worker not ready")))
	assert.True(t, isRetryableSchemaUpdateError(merr.WrapErrServiceInternal("transient fanout failure")))
	assert.True(t, isRetryableSchemaUpdateError(merr.SegcoreError(2018, "transient object storage failure")))
	assert.False(t, isRetryableSchemaUpdateError(grpcstatus.Error(codes.InvalidArgument, "invalid schema")))
	assert.False(t, isRetryableSchemaUpdateError(merr.WrapErrOperationNotSupportedMsg("unsupported schema")))
	assert.False(t, isRetryableSchemaUpdateError(merr.SegcoreError(2001, "invalid schema")))
	assert.False(t, isRetryableSchemaUpdateError(merr.SegcoreError(2020, "invalid field")))
}
