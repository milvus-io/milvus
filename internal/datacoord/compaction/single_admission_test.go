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

package compaction

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// singleAdmissionCandidate builds an accumulation candidate with the given
// deltalog count and deleted-rows ratio.
func singleAdmissionCandidate(id, collectionID int64, deltalogs int, deletedRatio float64) *policy.SingleCandidate {
	const rows = 10000
	binlogs := make([]*datapb.Binlog, 0, deltalogs)
	perLog := int64(deletedRatio * rows / float64(max(deltalogs, 1)))
	for i := 0; i < deltalogs; i++ {
		binlogs = append(binlogs, &datapb.Binlog{EntriesNum: perLog, LogSize: 10})
	}
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           id,
		CollectionID: collectionID,
		NumOfRows:    rows,
		Deltalogs:    []*datapb.FieldBinlog{{Binlogs: binlogs}},
	}}
	return policy.NewSingleCandidate(segment, policy.SingleReasonAccumulation)
}

func TestAdmissionBoundedByCapacityAndRefund(t *testing.T) {
	paramtable.Init()
	items := map[*paramtable.ParamItem]string{
		&Params.DataCoordCfg.SingleCompactionThresholdJitter:   "0",
		&Params.DataCoordCfg.SingleCompactionRateLimitTokens:   "10",
		&Params.DataCoordCfg.SingleCompactionRateLimitInterval: "60",
		&Params.DataCoordCfg.SingleCompactionDeltalogMaxNum:    "200",
	}
	for item, v := range items {
		saved := item.GetValue()
		key := item.Key
		t.Cleanup(func() { Params.Save(key, saved) })
		Params.Save(key, v)
	}
	a := policy.NewSingleCompactionAdmitterForTest()

	var cands []*policy.SingleCandidate
	for i := int64(1); i <= 10; i++ {
		cands = append(cands, singleAdmissionCandidate(i, 1, 5, 0.5))
	}
	admitted, deferred := a.Admit(context.Background(), policy.AdmissionSourceTrigger, cands, 3)
	assert.Len(t, admitted, 3)
	assert.Equal(t, 7, deferred)
	assert.Equal(t, 7.0, a.TokensForTest())

	round := newSingleAdmission(admitted, deferred, cands)
	round.enqueued(admitted[0].Segment().GetID(), 999) // one reached the queue, with an unrelated segment
	round.settle(a)
	assert.Equal(t, 9.0, a.TokensForTest(), "two admitted segments did not reach the queue and were refunded")

	// A nil admission (force signal) settles to nothing.
	var none *singleAdmission
	none.settle(a)
	assert.Equal(t, 9.0, a.TokensForTest())
}
