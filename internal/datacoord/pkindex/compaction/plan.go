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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
)

// plan is one compaction of one vchannel, as the inspector planned it.
type plan struct {
	target         manifest.Target
	sstBasePath    string
	baseVersion    int64
	rule           rule
	score          float64
	inputs         []*pkindexpb.SSTRef // manifest order, newest first
	inputBytes     int64
	dropTombstones bool
}

// makePlan applies the rules to m and copies the inputs out of it.
func makePlan(m *pkindexpb.Manifest, cfg Config) (plan, bool) {
	gs := groupSSTs(m.GetSsts())
	c, ok := evaluate(gs, cfg)
	if !ok {
		return plan{}, false
	}
	start, end := gs[c.first].start, gs[c.last].start+gs[c.last].tables
	inputs := make([]*pkindexpb.SSTRef, 0, end-start)
	var bytes int64
	for _, s := range m.GetSsts()[start:end] {
		inputs = append(inputs, proto.Clone(s).(*pkindexpb.SSTRef))
		bytes += s.GetSize()
	}
	return plan{
		target:         manifest.Target{CollectionID: m.GetCollectionId(), VChannel: m.GetVchannel()},
		sstBasePath:    m.GetSstBasePath(),
		baseVersion:    m.GetVersion(),
		rule:           c.rule,
		score:          c.score,
		inputs:         inputs,
		inputBytes:     bytes,
		dropTombstones: c.dropTombstones,
	}, true
}

// outputIDCount is the number of output IDs reserved for a compaction: twice
// the expected number of output tables plus four, so that uneven splits never
// run out.
func outputIDCount(inputBytes, targetFileSize int64) int64 {
	return (inputBytes+targetFileSize-1)/targetFileSize*2 + 4
}
