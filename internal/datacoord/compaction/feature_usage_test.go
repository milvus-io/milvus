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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestRecordCompactionType(t *testing.T) {
	featureusage.SetEnabled(true)
	snap := func() map[string]int64 {
		out := map[string]int64{}
		for _, e := range featureusage.SnapshotFor(featureusage.RoleMixCoord) {
			out[e.Name] = e.Value
		}
		return out
	}
	before := snap()
	recordCompactionType(datapb.CompactionType_ClusteringCompaction)
	recordCompactionType(datapb.CompactionType_MixCompaction)
	recordCompactionType(datapb.CompactionType_UndefinedCompaction)
	after := snap()
	assert.EqualValues(t, 1, after["compaction=clustering"]-before["compaction=clustering"])
	assert.EqualValues(t, 1, after["compaction=mix"]-before["compaction=mix"])
	assert.EqualValues(t, 1, after["compaction=_other"]-before["compaction=_other"])
	for name := range after {
		assert.Regexp(t, `^(import_file_type|compaction)=`, name, "MixCoord role holds only the DataCoord-side counters")
	}
}
