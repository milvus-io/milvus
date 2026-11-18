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
// request, ships the stages back with its SearchResults, and the receiving
// node merges them into its own Recorder, so the Proxy ends up with the whole
// request. A stage that runs many times (once per segment, worker or shard)
// is kept as one entry: the sum, the count and the slowest run.
package searchprofile

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// Stage names, in the order a search runs through them.
const (
	ProxyQueue      = "proxy.queue"
	ProxyPreExecute = "proxy.pre_execute"
	ProxyExecute    = "proxy.execute"
	// ProxyQueryPlan and ProxyTwoStageFilter run once per shard attempt before
	// the shard's work nodes are searched on the QueryView path.
	ProxyQueryPlan      = "proxy.query_plan"
	ProxyTwoStageFilter = "proxy.two_stage_filter"
	ProxyShardRPC       = "proxy.shard_rpc"
	QNWaitTSafe         = "qn.wait_tsafe"
	QNPinSegments       = "qn.pin_segments"
	QNFanout            = "qn.fanout"
	QNQueue             = "qn.queue"
	QNSegmentSearch     = "qn.segment_search"
	QNWorkerReduce      = "qn.worker_reduce"
	QNShardReduce       = "qn.shard_reduce"
	// ProxyShardReduce reduces each shard's work node results on the Proxy,
	// which the QueryView path does instead of a QueryNode delegator.
	ProxyShardReduce = "proxy.shard_reduce"
	ProxyPostExecute = "proxy.post_execute"

	// ProxyOpPrefix prefixes the stages of the Proxy's post-execute pipeline,
	// one per operator, e.g. "proxy.op.reduce".
	ProxyOpPrefix = "proxy.op."
	// OperatorPrefix prefixes the operators of a segment's search plan, which
	// run inside qn.segment_search, e.g. "op.PhyVectorSearchNode".
	OperatorPrefix = "op."
)

// Layers group the stages the way StarRocks groups a query profile, from the
// whole request down to the operators run on each segment.
const (
	LayerPlanner   = "Planner"
	LayerExecution = "Execution"
	LayerFragment  = "Fragment"
	LayerPipeline  = "Pipeline"
	LayerOperator  = "Operator"
)

var stageLayer = map[string]string{
	ProxyQueue:          LayerPlanner,
	ProxyPreExecute:     LayerPlanner,
	ProxyExecute:        LayerExecution,
	ProxyQueryPlan:      LayerExecution,
	ProxyTwoStageFilter: LayerExecution,
	ProxyShardReduce:    LayerExecution,
	ProxyPostExecute:    LayerExecution,
	ProxyShardRPC:       LayerFragment,
	QNWaitTSafe:         LayerFragment,
	QNPinSegments:       LayerFragment,
	QNFanout:            LayerFragment,
	QNShardReduce:       LayerFragment,
	QNQueue:             LayerPipeline,
	QNSegmentSearch:     LayerPipeline,
	QNWorkerReduce:      LayerPipeline,
}

// Layer returns the layer stage belongs to: the Proxy's planning, its
// execution, the work per shard, the work per segment on a QueryNode, or an
// operator of a segment's plan.
func Layer(stage string) string {
	if l, ok := stageLayer[stage]; ok {
		return l
	}
	if strings.HasPrefix(stage, ProxyOpPrefix) {
		return LayerExecution
	}
	if strings.HasPrefix(stage, OperatorPrefix) {
		return LayerOperator
	}
	return ""
}

var stageRank = func() map[string]int {
	order := []string{
		ProxyQueue, ProxyPreExecute, ProxyExecute, ProxyQueryPlan, ProxyTwoStageFilter, ProxyShardRPC,
		QNWaitTSafe, QNPinSegments, QNFanout, QNQueue, QNSegmentSearch, QNWorkerReduce, QNShardReduce,
		ProxyShardReduce, ProxyPostExecute,
	}
	rank := make(map[string]int, len(order))
	for i, s := range order {
		rank[s] = i
	}
	return rank
}()

// rank orders stages for display: known stages in execution order, pipeline
// operators right after the post-execute stage that runs them, segment plan
// operators right after the segment search that runs them, anything else
// last.
func rank(stage string) int {
	if r, ok := stageRank[stage]; ok {
		return r * 2
	}
	if strings.HasPrefix(stage, ProxyOpPrefix) {
		return stageRank[ProxyPostExecute]*2 + 1
	}
	if strings.HasPrefix(stage, OperatorPrefix) {
		return stageRank[QNSegmentSearch]*2 + 1
	}
	return len(stageRank) * 2
}

// Recorder collects the stages of one search request on one node. It is safe
// for concurrent use, and a nil Recorder ignores every call, so code on the
// search path never has to check whether a request is being recorded.
type Recorder struct {
	nodeID int64

	mu     sync.Mutex
	stages []*internalpb.StageCost
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
	r.merge(&internalpb.StageCost{
		Stage:     stage,
		SumUs:     us,
		MaxUs:     us,
		Count:     1,
		MaxNodeId: r.nodeID,
		MaxId:     id,
	})
}

// ObserveSince records one run of stage that started at start and ends now.
func (r *Recorder) ObserveSince(stage string, start time.Time, id int64) {
	if r == nil {
		return
	}
	r.Observe(stage, time.Since(start), id)
}

// Merge adds the stages another node reported for the same request.
func (r *Recorder) Merge(costs []*internalpb.StageCost) {
	if r == nil || len(costs) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range costs {
		r.merge(c)
	}
}

func (r *Recorder) merge(c *internalpb.StageCost) {
	i, ok := r.index[c.GetStage()]
	if !ok {
		r.index[c.GetStage()] = len(r.stages)
		r.stages = append(r.stages, &internalpb.StageCost{
			Stage:     c.GetStage(),
			SumUs:     c.GetSumUs(),
			MaxUs:     c.GetMaxUs(),
			Count:     c.GetCount(),
			MaxNodeId: c.GetMaxNodeId(),
			MaxId:     c.GetMaxId(),
		})
		return
	}
	s := r.stages[i]
	s.SumUs += c.GetSumUs()
	s.Count += c.GetCount()
	if c.GetMaxUs() > s.MaxUs {
		s.MaxUs = c.GetMaxUs()
		s.MaxNodeId = c.GetMaxNodeId()
		s.MaxId = c.GetMaxId()
	}
}

// Stages returns a copy of the recorded stages in execution order.
func (r *Recorder) Stages() []*internalpb.StageCost {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.stages) == 0 {
		return nil
	}
	out := make([]*internalpb.StageCost, 0, len(r.stages))
	for _, s := range r.stages {
		out = append(out, &internalpb.StageCost{
			Stage:     s.GetStage(),
			SumUs:     s.GetSumUs(),
			MaxUs:     s.GetMaxUs(),
			Count:     s.GetCount(),
			MaxNodeId: s.GetMaxNodeId(),
			MaxId:     s.GetMaxId(),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return rank(out[i].GetStage()) < rank(out[j].GetStage())
	})
	return out
}

type recorderKey struct{}

// WithRecorder returns ctx carrying r. Code deeper in the call chain records
// into it through FromContext; pass a nil r to stop a callee from recording
// into a caller's Recorder.
func WithRecorder(ctx context.Context, r *Recorder) context.Context {
	return context.WithValue(ctx, recorderKey{}, r)
}

// FromContext returns the Recorder ctx carries, or nil.
func FromContext(ctx context.Context) *Recorder {
	r, _ := ctx.Value(recorderKey{}).(*Recorder)
	return r
}

// Format renders stages on one line for a log entry. A stage that ran once
// shows its time; one that ran several times shows the count, the sum and
// where the slowest run happened.
func Format(stages []*internalpb.StageCost) string {
	var b strings.Builder
	for i, s := range stages {
		if i > 0 {
			b.WriteString("; ")
		}
		if s.GetCount() <= 1 {
			fmt.Fprintf(&b, "%s=%s", s.GetStage(), formatUs(s.GetSumUs()))
			continue
		}
		fmt.Fprintf(&b, "%s: n=%d sum=%s max=%s (node=%d", s.GetStage(), s.GetCount(),
			formatUs(s.GetSumUs()), formatUs(s.GetMaxUs()), s.GetMaxNodeId())
		if s.GetMaxId() != 0 {
			fmt.Fprintf(&b, " id=%d", s.GetMaxId())
		}
		b.WriteString(")")
	}
	return b.String()
}

// ToSlowQueryStages converts stages for the slow query record.
func ToSlowQueryStages(stages []*internalpb.StageCost) []*metricsinfo.SearchStage {
	if len(stages) == 0 {
		return nil
	}
	out := make([]*metricsinfo.SearchStage, 0, len(stages))
	for _, s := range stages {
		out = append(out, &metricsinfo.SearchStage{
			Stage:     s.GetStage(),
			Layer:     Layer(s.GetStage()),
			Count:     s.GetCount(),
			Sum:       formatUs(s.GetSumUs()),
			Max:       formatUs(s.GetMaxUs()),
			MaxNodeID: s.GetMaxNodeId(),
			MaxID:     s.GetMaxId(),
		})
	}
	return out
}

// Report is the profile of one search returned to a client that asked for it.
type Report struct {
	Summary *Summary                   `json:"summary"`
	Stages  []*metricsinfo.SearchStage `json:"stages"`
}

// Summary describes the request a profile is of, as the Summary of a
// StarRocks query profile does.
type Summary struct {
	// Total is the request's time on the Proxy, from receipt to result.
	Total      string `json:"total"`
	Type       string `json:"type"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
	// QueryParams holds the expression, the search params and the nq of the
	// request, or of each sub-request of a hybrid search.
	QueryParams *metricsinfo.QueryParams `json:"query_params,omitempty"`
	// Segments is how many segment searches served the request.
	Segments int64 `json:"segments"`
}

// MarshalReport renders the profile of a request that took total on the
// Proxy as JSON. summary describes the request; its Total and Segments are
// filled in here.
func MarshalReport(summary *Summary, total time.Duration, stages []*internalpb.StageCost) (string, error) {
	summary.Total = formatUs(total.Microseconds())
	for _, s := range stages {
		if s.GetStage() == QNSegmentSearch {
			summary.Segments = s.GetCount()
		}
	}
	report := Report{
		Summary: summary,
		Stages:  ToSlowQueryStages(stages),
	}
	if report.Stages == nil {
		report.Stages = []*metricsinfo.SearchStage{}
	}
	b, err := json.Marshal(report)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func formatUs(us int64) string {
	return fmt.Sprintf("%.3fms", float64(us)/1000)
}
