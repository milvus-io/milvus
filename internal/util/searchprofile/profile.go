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

// Package searchprofile records how long each stage of a search request takes
// on every node that serves it. Each node keeps a Recorder for its part of the
// request, ships the stages back in the Status of its SearchResults, and the
// receiving node merges them into its own Recorder, so the Proxy ends up with
// the whole request. A stage that runs many times (once per segment, worker
// or shard) is kept as one entry: the sum, the count and the slowest run.
package searchprofile

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// Stage names, in the order a search runs through them.
const (
	ProxyQueue      = "proxy.queue"
	ProxyPreExecute = "proxy.pre_execute"
	ProxyExecute    = "proxy.execute"
	// ProxyQueryPlan runs once per shard attempt before the shard's work nodes
	// are searched, each in a ProxyShardRPC.
	ProxyQueryPlan = "proxy.query_plan"
	ProxyShardRPC  = "proxy.shard_rpc"
	// QNExecute is a work node's whole search for one request, of which
	// QNSegmentSearch is the part spent searching each segment.
	QNExecute        = "qn.execute"
	QNSegmentSearch  = "qn.segment_search"
	ProxyPostExecute = "proxy.post_execute"

	// OperatorPrefix prefixes the operators of a segment's search plan, which
	// run inside qn.segment_search, e.g. "op.PhyVectorSearchNode".
	OperatorPrefix = "op."
)

// ExtraInfoKey is the key of a work node's stages in the Status.ExtraInfo of
// its SearchResults.
const ExtraInfoKey = "search_stages"

// Stage is the time one stage of a request took, merged over every run of the
// stage (one per segment, worker or shard).
type Stage struct {
	Name  string `json:"s"`
	SumUs int64  `json:"sum"`
	MaxUs int64  `json:"max"`
	Count int64  `json:"n"`
	// where the slowest run happened: its node, and the segment or node it ran
	// on when the stage runs per segment or shard
	MaxNodeID int64 `json:"node,omitempty"`
	MaxID     int64 `json:"id,omitempty"`
}

// Recorder collects the stages of one search request on one node. It is safe
// for concurrent use, and a nil Recorder ignores every call, so code on the
// search path never has to check whether a request is being recorded.
type Recorder struct {
	nodeID int64

	mu     sync.Mutex
	stages []Stage
	index  map[string]int
}

// NewRecorder returns an empty Recorder for the node nodeID.
func NewRecorder(nodeID int64) *Recorder {
	return &Recorder{nodeID: nodeID, index: make(map[string]int)}
}

// Child returns an empty Recorder for the same node, or nil when r is nil. It
// holds stages that count only if the work they time is kept, such as one
// attempt of a shard that may be retried; Merge its Stages into r once it is.
func (r *Recorder) Child() *Recorder {
	if r == nil {
		return nil
	}
	return NewRecorder(r.nodeID)
}

// Observe records one run of stage that took d. id names what the run was on
// (a segment, a shard's node), or is 0.
func (r *Recorder) Observe(stage string, d time.Duration, id int64) {
	if r == nil {
		return
	}
	us := d.Microseconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.merge(Stage{Name: stage, SumUs: us, MaxUs: us, Count: 1, MaxNodeID: r.nodeID, MaxID: id})
}

// ObserveSince records one run of stage that started at start and ends now.
func (r *Recorder) ObserveSince(stage string, start time.Time, id int64) {
	if r == nil {
		return
	}
	r.Observe(stage, time.Since(start), id)
}

// Merge adds the stages another node reported for the same request.
func (r *Recorder) Merge(stages []Stage) {
	if r == nil || len(stages) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range stages {
		r.merge(s)
	}
}

func (r *Recorder) merge(s Stage) {
	i, ok := r.index[s.Name]
	if !ok {
		r.index[s.Name] = len(r.stages)
		r.stages = append(r.stages, s)
		return
	}
	cur := &r.stages[i]
	cur.SumUs += s.SumUs
	cur.Count += s.Count
	if s.MaxUs > cur.MaxUs {
		cur.MaxUs = s.MaxUs
		cur.MaxNodeID = s.MaxNodeID
		cur.MaxID = s.MaxID
	}
}

// Stages returns a copy of the recorded stages, in the order each was first
// recorded. A stage is recorded when it ends, so a stage follows the stages
// that ran inside it.
func (r *Recorder) Stages() []Stage {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.stages) == 0 {
		return nil
	}
	return append([]Stage(nil), r.stages...)
}

type recorderKey struct{}

// WithRecorder returns ctx carrying r. Code deeper in the call chain records
// into it through FromContext.
func WithRecorder(ctx context.Context, r *Recorder) context.Context {
	return context.WithValue(ctx, recorderKey{}, r)
}

// FromContext returns the Recorder ctx carries, or nil.
func FromContext(ctx context.Context) *Recorder {
	r, _ := ctx.Value(recorderKey{}).(*Recorder)
	return r
}

// Attach puts stages into status for the node that receives it. It does
// nothing when there are no stages or no status.
func Attach(status *commonpb.Status, stages []Stage) {
	if status == nil || len(stages) == 0 {
		return
	}
	b, err := json.Marshal(stages)
	if err != nil {
		return
	}
	if status.ExtraInfo == nil {
		status.ExtraInfo = make(map[string]string)
	}
	status.ExtraInfo[ExtraInfoKey] = string(b)
}

// Take returns the stages Attach put into status and removes them, so that
// they do not travel any further with the result.
func Take(status *commonpb.Status) []Stage {
	encoded, ok := status.GetExtraInfo()[ExtraInfoKey]
	if !ok {
		return nil
	}
	delete(status.ExtraInfo, ExtraInfoKey)
	var stages []Stage
	if err := json.Unmarshal([]byte(encoded), &stages); err != nil {
		return nil
	}
	return stages
}

// Format renders stages on one line for a log entry. A stage that ran once
// shows its time; one that ran several times shows the count, the sum and
// where the slowest run happened.
func Format(stages []Stage) string {
	var b strings.Builder
	for i, s := range stages {
		if i > 0 {
			b.WriteString("; ")
		}
		if s.Count <= 1 {
			fmt.Fprintf(&b, "%s=%s", s.Name, formatUs(s.SumUs))
			continue
		}
		fmt.Fprintf(&b, "%s: n=%d sum=%s max=%s (node=%d", s.Name, s.Count,
			formatUs(s.SumUs), formatUs(s.MaxUs), s.MaxNodeID)
		if s.MaxID != 0 {
			fmt.Fprintf(&b, " id=%d", s.MaxID)
		}
		b.WriteString(")")
	}
	return b.String()
}

func formatUs(us int64) string {
	return fmt.Sprintf("%.3fms", float64(us)/1000)
}
