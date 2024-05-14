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
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/integration"
)

type CompactionSuite struct {
	integration.MiniClusterSuite
}

func waitingForCompacted(ctx context.Context, metaWatcher integration.MetaWatcher, t *testing.T) {
	showSegments := func() bool {
		segments, err := metaWatcher.ShowSegments()
		assert.NoError(t, err)
		assert.NotEmpty(t, segments)
		compactFromSegments := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
			return segment.GetState() == commonpb.SegmentState_Dropped
		})
		compactToSegments := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
			return segment.GetState() == commonpb.SegmentState_Flushed
		})
		log.Info("ShowSegments result", zap.Int("len(compactFromSegments)", len(compactFromSegments)),
			zap.Int("len(compactToSegments)", len(compactToSegments)))
		return len(compactToSegments) == 1
	}
	for !showSegments() {
		select {
		case <-ctx.Done():
			log.Fatal("waiting for compaction failed") // TODO: test timeout
			t.FailNow()
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func TestCompaction(t *testing.T) {
	suite.Run(t, new(CompactionSuite))
}
