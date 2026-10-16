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
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func (suite *PipelineTestSuite) newTimeTickTestPipeline() (Pipeline, *schemapb.CollectionSchema, chan *msgstream.MsgPack) {
	t := suite.T()
	t.Helper()
	params := paramtable.Get()
	for _, setting := range []struct {
		param *paramtable.ParamItem
		value string
	}{
		{&params.StreamingCfg.DelegatorEmptyTimeTickMaxFilterInterval, "2s"},
		{&params.QueryNodeCfg.DMLMicroBatchMaxMsgNum, "8"},
	} {
		key, previous := setting.param.Key, setting.param.GetValue()
		suite.Require().NoError(params.Save(key, setting.value))
		t.Cleanup(func() { assert.NoError(t, params.Save(key, previous)) })
	}

	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, true)
	collection, err := segments.NewCollection(suite.collectionID, schema, mock_segcore.GenTestIndexMeta(suite.collectionID, schema), &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	})
	suite.Require().NoError(err)
	suite.T().Cleanup(func() { segments.DeleteCollection(collection) })
	suite.collectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	// Queue complete batches before starting the worker so batching does not
	// depend on goroutine scheduling.
	msgChan := make(chan *msgstream.MsgPack, 4)
	suite.msgDispatcher.EXPECT().Register(mock.Anything, mock.Anything).Return(msgChan, nil).Once()
	suite.msgDispatcher.EXPECT().Deregister(suite.channel).Once()
	// An earlier read was already satisfied by the initial empty tick at 100ms.
	// No new read demand rescues the later empty tick from the slowdowner.
	suite.delegator.EXPECT().GetLatestRequiredMVCCTimeTick().Return(tsoutil.ComposeTS(90, 0))

	p, err := NewPipeLine(collection, suite.channel, &segments.Manager{
		Collection: suite.collectionManager,
		Segment:    suite.segmentManager,
	}, suite.msgDispatcher, suite.delegator)
	suite.Require().NoError(err)
	suite.T().Cleanup(p.Close)
	suite.Require().NoError(p.ConsumeMsgStream(context.Background(), &msgpb.MsgPosition{}))
	return p, schema, msgChan
}

func (suite *PipelineTestSuite) requireTSafeUpdate(updates <-chan uint64, expected uint64) {
	suite.T().Helper()
	select {
	case ts := <-updates:
		suite.Require().Equal(expected, ts)
	case <-time.After(5 * time.Second):
		suite.FailNow("pipeline did not advance consumption progress", "expected tSafe %d", expected)
	}
}

func (suite *PipelineTestSuite) TestFilteredEmptyTimeTickAdvancesTSafe() {
	p, schema, msgChan := suite.newTimeTickTestPipeline()
	initialTS := tsoutil.ComposeTS(100, 0)
	insertTS := tsoutil.ComposeTS(120, 0)
	confirmedTS := tsoutil.ComposeTS(300, 0)

	updates := make(chan uint64, 4)
	suite.delegator.EXPECT().UpdateTSafe(mock.Anything).Run(func(ts uint64) { updates <- ts })
	fullPipeline := make(chan uint64, 4)
	suite.delegator.EXPECT().TryCleanExcludedSegments(mock.Anything).Run(func(ts uint64) { fullPipeline <- ts })
	suite.delegator.EXPECT().VerifyExcludedSegments(suite.insertSegmentIDs[0], insertTS).Return(true).Once()
	inserted := make(chan map[int64]*delegator.InsertData, 1)
	suite.delegator.EXPECT().ProcessInsert(mock.Anything).Run(func(data map[int64]*delegator.InsertData) { inserted <- data }).Once()

	insert := buildInsertMsg(suite.collectionID, suite.partitionIDs[0], suite.insertSegmentIDs[0], suite.channel, 1)
	insert.SetTs(insertTS)
	insert.Timestamps = []uint64{insertTS}
	insert.RowIDs = []int64{1}
	insert.FieldsData = genFiledDataWithSchema(schema, 1)
	msgChan <- &msgstream.MsgPack{BeginTs: initialTS, EndTs: initialTS}
	msgChan <- &msgstream.MsgPack{BeginTs: insertTS, EndTs: insertTS, Msgs: []msgstream.TsMsg{insert}}
	msgChan <- &msgstream.MsgPack{BeginTs: confirmedTS, EndTs: confirmedTS}
	suite.Require().NoError(p.Start())

	suite.requireTSafeUpdate(updates, initialTS)
	suite.requireTSafeUpdate(updates, insertTS)
	select {
	case data := <-inserted:
		suite.Require().Len(data, 1)
		suite.Require().Contains(data, suite.insertSegmentIDs[0])
		suite.Equal([]uint64{insertTS}, data[suite.insertSegmentIDs[0]].Timestamps)
		suite.Equal([]int64{1}, data[suite.insertSegmentIDs[0]].RowIDs)
		suite.Equal(int64(1), data[suite.insertSegmentIDs[0]].InsertRecord.NumRows)
	case <-time.After(5 * time.Second):
		suite.FailNow("insert did not reach the delegator")
	}
	// The WAL's confirmed tick must become visible without a new read demand
	// or another periodic tick, even though the expensive nodes are skipped.
	suite.requireTSafeUpdate(updates, confirmedTS)
	p.Close()
	suite.Require().Len(fullPipeline, 2)
	suite.Equal(initialTS, <-fullPipeline)
	suite.Equal(insertTS, <-fullPipeline)
	suite.Empty(updates)
}

func (suite *PipelineTestSuite) TestFilteredEmptyTimeTickWaitsForDeleteMicrobatch() {
	p, _, msgChan := suite.newTimeTickTestPipeline()
	initialTS := tsoutil.ComposeTS(100, 0)
	firstDeleteTS := tsoutil.ComposeTS(120, 0)
	secondDeleteTS := tsoutil.ComposeTS(140, 0)
	confirmedTS := tsoutil.ComposeTS(300, 0)

	updates := make(chan uint64, 4)
	suite.delegator.EXPECT().UpdateTSafe(mock.Anything).Run(func(ts uint64) { updates <- ts })
	fullPipeline := make(chan uint64, 4)
	suite.delegator.EXPECT().TryCleanExcludedSegments(mock.Anything).Run(func(ts uint64) { fullPipeline <- ts })
	deleteEntered := make(chan []delegator.DeleteBatch, 1)
	releaseDelete := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseDelete) }) }
	// Unblock the worker before the pipeline cleanup, including on assertion failures.
	suite.T().Cleanup(release)
	suite.delegator.EXPECT().ProcessDeleteBatches(mock.Anything).Run(func(batches []delegator.DeleteBatch) {
		deleteEntered <- batches
		<-releaseDelete
	}).Once()

	msgChan <- &msgstream.MsgPack{BeginTs: initialTS, EndTs: initialTS}
	for i, ts := range []uint64{firstDeleteTS, secondDeleteTS} {
		msg := buildDeleteMsg(suite.collectionID, suite.partitionIDs[0], suite.channel, 1)
		msg.SetTs(ts)
		msg.Timestamps = []uint64{ts}
		msg.PrimaryKeys = genDeletePK(int64(i + 1))
		msgChan <- &msgstream.MsgPack{BeginTs: ts, EndTs: ts, Msgs: []msgstream.TsMsg{msg}}
	}
	// The batcher retains this empty pack as pending after draining both deletes.
	msgChan <- &msgstream.MsgPack{BeginTs: confirmedTS, EndTs: confirmedTS}
	suite.Require().NoError(p.Start())

	suite.requireTSafeUpdate(updates, initialTS)
	select {
	case batches := <-deleteEntered:
		suite.Require().Len(batches, 2)
		suite.Equal(firstDeleteTS, batches[0].Ts)
		suite.Equal(secondDeleteTS, batches[1].Ts)
		for i, batch := range batches {
			suite.Require().Len(batch.Data, 1)
			suite.Require().Len(batch.Data[0].PrimaryKeys, 1)
			suite.Equal(int64(i+1), batch.Data[0].PrimaryKeys[0].GetValue())
			suite.Equal([]uint64{batch.Ts}, batch.Data[0].Timestamps)
		}
	case <-time.After(5 * time.Second):
		suite.FailNow("delete microbatch did not reach the delegator")
	}
	select {
	case ts := <-updates:
		suite.FailNow("tSafe advanced before deletes finished", "tSafe %d", ts)
	case <-time.After(50 * time.Millisecond):
	}

	release()
	suite.requireTSafeUpdate(updates, secondDeleteTS)
	suite.requireTSafeUpdate(updates, confirmedTS)
	p.Close()
	suite.Require().Len(fullPipeline, 2)
	suite.Equal(initialTS, <-fullPipeline)
	suite.Equal(secondDeleteTS, <-fullPipeline)
	suite.Empty(updates)
}

func (suite *PipelineTestSuite) TestFilteredEmptyTimeTickWaitsForSchemaUpdate() {
	p, schema, msgChan := suite.newTimeTickTestPipeline()
	initialTS := tsoutil.ComposeTS(100, 0)
	schemaTS := tsoutil.ComposeTS(120, 0)
	confirmedTS := tsoutil.ComposeTS(300, 0)

	updates := make(chan uint64, 4)
	suite.delegator.EXPECT().UpdateTSafe(mock.Anything).Run(func(ts uint64) { updates <- ts })
	fullPipeline := make(chan uint64, 4)
	suite.delegator.EXPECT().TryCleanExcludedSegments(mock.Anything).Run(func(ts uint64) { fullPipeline <- ts })
	schemaEntered := make(chan *schemapb.CollectionSchema, 1)
	releaseSchema := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSchema) }) }
	// Unblock the worker before the pipeline cleanup, including on assertion failures.
	suite.T().Cleanup(release)
	suite.delegator.EXPECT().UpdateSchema(mock.Anything, mock.Anything, schemaTS).
		RunAndReturn(func(_ context.Context, updated *schemapb.CollectionSchema, _ uint64) error {
			schemaEntered <- updated
			<-releaseSchema
			return nil
		}).Once()

	updatedSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	updatedSchema.Version++
	updatedSchema.Fields = append(updatedSchema.Fields, &schemapb.FieldSchema{
		FieldID:  200,
		Name:     "added_field",
		DataType: schemapb.DataType_Int64,
		Nullable: true,
	})
	schemaMessage := message.NewSchemaChangeMessageBuilderV2().
		WithVChannel(suite.channel).
		WithHeader(&messagespb.SchemaChangeMessageHeader{CollectionId: suite.collectionID}).
		WithBody(&messagespb.SchemaChangeMessageBody{Schema: updatedSchema}).
		MustBuildMutable().
		WithTimeTick(schemaTS).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(1))
	schemaChange, err := adaptor.NewSchemaChangeMessageBody(schemaMessage)
	suite.Require().NoError(err)
	msgChan <- &msgstream.MsgPack{BeginTs: initialTS, EndTs: initialTS}
	msgChan <- &msgstream.MsgPack{BeginTs: schemaTS, EndTs: schemaTS, Msgs: []msgstream.TsMsg{schemaChange}}
	msgChan <- &msgstream.MsgPack{BeginTs: confirmedTS, EndTs: confirmedTS}
	suite.Require().NoError(p.Start())

	suite.requireTSafeUpdate(updates, initialTS)
	select {
	case updated := <-schemaEntered:
		suite.True(proto.Equal(updatedSchema, updated), "pipeline must forward the complete updated schema")
	case <-time.After(5 * time.Second):
		suite.FailNow("schema update did not reach the delegator")
	}
	select {
	case ts := <-updates:
		suite.FailNow("tSafe advanced before schema update finished", "tSafe %d", ts)
	case <-time.After(50 * time.Millisecond):
	}

	release()
	suite.requireTSafeUpdate(updates, schemaTS)
	suite.requireTSafeUpdate(updates, confirmedTS)
	p.Close()
	suite.Require().Len(fullPipeline, 2)
	suite.Equal(initialTS, <-fullPipeline)
	suite.Equal(schemaTS, <-fullPipeline)
	suite.Empty(updates)
}
