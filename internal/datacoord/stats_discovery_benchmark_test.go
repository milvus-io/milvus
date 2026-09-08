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

package datacoord

import (
	"fmt"
	"iter"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func BenchmarkStatsDiscoverySteady(b *testing.B) {
	for _, size := range []int{1000, 100000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			f := newDiscoveryFixture(b, "event")
			for id := int64(1); id <= int64(size); id++ {
				segment := discoverySegment(id, true)
				segment.TextStatsLogs = map[int64]*datapb.TextIndexStats{101: {}}
				segment.JsonKeyStats = map[int64]*datapb.JsonKeyStats{102: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion}}
				f.mt.segments.SetSegment(id, segment)
			}
			b.Run("poll_with_direct_field_check", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					f.si.triggerStatsTasks(0)
				}
			})
			b.Run("event_no_change", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					f.si.processStatsDiscoveryBatch()
				}
			})
			b.Run("stream_first_entry", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					next, stop := iter.Pull2(f.mt.rangeStatsSegments(0))
					next()
					stop()
				}
			})
		})
	}
}
