// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package delegator

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestLoadSealedForReopenSerializesDifferentFieldsForSameSegment(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}, {
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{103},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	field102 := genBM25StatsForField(102, 1, 3)
	data102, err := field102[102].Serialize()
	require.NoError(t, err)
	field104 := genBM25StatsForField(104, 10, 13)
	data104, err := field104[104].Serialize()
	require.NoError(t, err)

	path102 := "bm25stats/seg_1/field_102/0"
	path104 := "bm25stats/seg_1/field_104/0"
	firstReadStarted := make(chan struct{})
	releaseFirstRead := make(chan struct{})
	secondReadStarted := make(chan struct{})
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, path102).RunAndReturn(func(context.Context, string) (storage.FileReader, error) {
		close(firstReadStarted)
		<-releaseFirstRead
		return &bytesFileReader{bytes.NewReader(data102)}, nil
	}).Once()
	cm.EXPECT().Reader(mock.Anything, path104).RunAndReturn(func(context.Context, string) (storage.FileReader, error) {
		close(secondReadStarted)
		return &bytesFileReader{bytes.NewReader(data104)}, nil
	}).Once()

	errs := make(chan error, 2)
	go func() {
		errs <- idfOracle.LoadSealedForReopen(context.Background(), 1, &querypb.SegmentLoadInfo{
			Bm25Logs: bm25LogsForField(102, path102),
		}, cm, false)
	}()
	select {
	case <-firstReadStarted:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "first field load did not start")
	}
	secondCallStarted := make(chan struct{})
	go func() {
		close(secondCallStarted)
		errs <- idfOracle.LoadSealedForReopen(context.Background(), 1, &querypb.SegmentLoadInfo{
			Bm25Logs: bm25LogsForField(104, path104),
		}, cm, false)
	}()
	<-secondCallStarted
	select {
	case <-secondReadStarted:
		require.FailNow(t, "second field read started before the first load released")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirstRead)
	select {
	case <-secondReadStarted:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "second field load did not start after the first released")
	}

	for range 2 {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			require.FailNow(t, "BM25 field load did not finish")
		}
	}
	sealedStats, ok := idfOracle.sealed.Get(1)
	require.True(t, ok)
	assert.ElementsMatch(t, []int64{102, 104}, sealedStats.FieldList())
	fetched, err := sealedStats.FetchStats()
	require.NoError(t, err)
	assert.Equal(t, int64(2), fetched[102].NumRow())
	assert.Equal(t, int64(3), fetched[104].NumRow())
}

func TestLoadSealedForReopenLockWaitIsContextCancelable(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", nil).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	idfOracle.segmentLoadLock.Lock(1)
	defer idfOracle.segmentLoadLock.Unlock(1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- idfOracle.LoadSealedForReopen(ctx, 1, &querypb.SegmentLoadInfo{}, nil, false)
	}()

	select {
	case err := <-done:
		require.Failf(t, "load returned before cancellation", "error: %v", err)
	case <-time.After(2 * segmentLoadLockPollInterval):
	}
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		require.Fail(t, "load did not stop after context cancellation")
	}
}
