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

package replicatestream

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func newTestReplicateInfo(source, target string) *streamingpb.ReplicatePChannelMeta {
	return &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: source,
		TargetChannelName: target,
		TargetCluster: &commonpb.MilvusCluster{
			ClusterId: "test-cluster",
		},
	}
}

// TestInitLastReplicatedTimeTick verifies that seeding from the resume
// checkpoint reports the real backlog (checkpoint physical time), so a
// restarted CDC pod does not read as 0.
func TestInitLastReplicatedTimeTick(t *testing.T) {
	source, target := "TestInit-source", "TestInit-target"
	defer metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target)

	// A checkpoint 300s in the past -> the gauge reports that physical time,
	// which against wall-clock now is a ~300s backlog, not 0.
	checkpoint := tsoutil.ComposeTSByTime(time.Now().Add(-300*time.Second), 0)
	InitLastReplicatedTimeTick(newTestReplicateInfo(source, target), checkpoint)

	got := testutil.ToFloat64(metrics.CDCLastReplicatedTimeTick.WithLabelValues(source, target))
	assert.InDelta(t, tsoutil.PhysicalTimeSeconds(checkpoint), got, 1)
	assert.InDelta(t, 300, float64(time.Now().Unix())-got, 5)
}

func TestInitLastReplicatedTimeTickIgnoresZero(t *testing.T) {
	source, target := "TestInitZero-source", "TestInitZero-target"

	InitLastReplicatedTimeTick(newTestReplicateInfo(source, target), 0)

	assert.False(t, metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target))
}

func TestInitLastReplicatedTimeTickIgnoresNilInfo(t *testing.T) {
	assert.NotPanics(t, func() {
		InitLastReplicatedTimeTick(nil, tsoutil.ComposeTSByTime(time.Now(), 0))
	})
}

func TestReplicateMetricsOnCloseKeepsLastReplicatedSeries(t *testing.T) {
	source, target := "TestOnClose-source", "TestOnClose-target"

	m := NewReplicateMetrics(newTestReplicateInfo(source, target))
	m.UpdateLastReplicatedTimeTick(tsoutil.ComposeTSByTime(time.Now(), 0))

	m.OnClose()

	assert.True(t, metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target))
}

func TestDeleteLastReplicatedTimeTickDeletesExactSeries(t *testing.T) {
	source, target := "TestDelete-source", "TestDelete-target"
	info := newTestReplicateInfo(source, target)
	InitLastReplicatedTimeTick(info, tsoutil.ComposeTSByTime(time.Now(), 0))

	DeleteLastReplicatedTimeTick(info)

	assert.False(t, metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target))
}
