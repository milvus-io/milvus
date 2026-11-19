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

package searchprofile

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// The colors StarRocks' ANALYZE PROFILE marks its hot nodes with.
const (
	colorHot   = "\x1b[1m\x1b[31m"
	colorWarm  = "\x1b[1m\x1b[38;2;250;128;114m"
	colorReset = "\x1b[0m"
)

// stageParent is the stage each stage runs inside. A stage whose parent did
// not run, such as qn.fanout on the QueryView path, hangs under the nearest
// ancestor that did.
var stageParent = map[string]string{
	ProxyQueryPlan:      ProxyExecute,
	ProxyTwoStageFilter: ProxyExecute,
	ProxyShardRPC:       ProxyExecute,
	ProxyShardReduce:    ProxyExecute,
	QNWaitTSafe:         ProxyShardRPC,
	QNPinSegments:       ProxyShardRPC,
	QNFanout:            ProxyShardRPC,
	QNShardReduce:       ProxyShardRPC,
	QNQueue:             QNFanout,
	QNSegmentSearch:     QNFanout,
	QNWorkerReduce:      QNFanout,
}

func parentOf(stage string) string {
	if strings.HasPrefix(stage, OperatorPrefix) {
		return QNSegmentSearch
	}
	if strings.HasPrefix(stage, ProxyOpPrefix) {
		return ProxyPostExecute
	}
	return stageParent[stage]
}

type renderNode struct {
	cost     *internalpb.StageCost
	selfUs   int64
	children []*renderNode
}

// Render draws a profile the way StarRocks' ANALYZE PROFILE draws a query
// profile: a summary that lists the stages that took the most time, then the
// stages as a tree, each with its time and its share of the total. A stage
// whose own time, without the stages inside it, is over 30% of the total is
// red, and one at 15-30% salmon.
func Render(summary *Summary, total time.Duration, stages []*internalpb.StageCost) string {
	totalUs := total.Microseconds()
	byName := make(map[string]*renderNode, len(stages))
	for _, s := range stages {
		byName[s.GetStage()] = &renderNode{cost: s}
	}
	var roots []*renderNode
	for _, s := range stages {
		n := byName[s.GetStage()]
		parent := parentOf(s.GetStage())
		for parent != "" && byName[parent] == nil {
			parent = parentOf(parent)
		}
		if parent == "" {
			roots = append(roots, n)
			continue
		}
		byName[parent].children = append(byName[parent].children, n)
	}
	for _, n := range byName {
		n.selfUs = n.cost.GetSumUs()
		for _, c := range n.children {
			n.selfUs -= c.cost.GetSumUs()
		}
		// children that ran in parallel can add up to more than their parent
		n.selfUs = max(n.selfUs, 0)
	}

	share := func(us int64) float64 {
		if totalUs <= 0 {
			return 0
		}
		return float64(us) * 100 / float64(totalUs)
	}
	color := func(selfUs int64) string {
		switch p := share(selfUs); {
		case p > 30:
			return colorHot
		case p >= 15:
			return colorWarm
		}
		return ""
	}
	paint := func(c, s string) string {
		if c == "" {
			return colorReset + s + colorReset
		}
		return c + s + colorReset
	}

	var b strings.Builder
	line := func(c, s string) { b.WriteString(paint(c, s) + "\n") }

	line("", "Summary")
	if summary != nil {
		line("", "    Type: "+summary.Type)
		line("", "    Collection: "+summary.Database+"."+summary.Collection)
	}
	line("", "    State: Finished")
	line("", "    TotalTime: "+formatUs(totalUs))
	if summary != nil {
		line("", fmt.Sprintf("    Segments: %d", summary.Segments))
	}
	top := make([]*renderNode, 0, len(byName))
	for _, n := range byName {
		if n.selfUs > 0 {
			top = append(top, n)
		}
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].selfUs != top[j].selfUs {
			return top[i].selfUs > top[j].selfUs
		}
		return top[i].cost.GetStage() < top[j].cost.GetStage()
	})
	line("", "    Top Most Time-consuming Stages:")
	for i, n := range top[:min(5, len(top))] {
		line(color(n.selfUs), fmt.Sprintf("        %d. %s : %s (%.2f%%)", i+1, n.cost.GetStage(), formatUs(n.selfUs), share(n.selfUs)))
	}
	if summary != nil && summary.QueryParams != nil {
		line("", "    QueryParams:")
		line("", "        Expr: "+summary.QueryParams.Expr)
		for _, p := range summary.QueryParams.SearchParams {
			line("", fmt.Sprintf("        NQ: %v, SearchParams: %s", p.NQ, strings.Join(p.SearchParams, ";")))
		}
	}

	line("", "Stages")
	var walk func(nodes []*renderNode, prefix string)
	walk = func(nodes []*renderNode, prefix string) {
		for i, n := range nodes {
			last := i == len(nodes)-1
			branch, cont := "├──", "│  "
			if last {
				branch, cont = "└──", "   "
			}
			c := color(n.selfUs)
			s := n.cost
			line(c, fmt.Sprintf("%s%s%s [%s]", prefix, branch, s.GetStage(), Layer(s.GetStage())))
			bar := "    "
			if len(n.children) > 0 {
				bar = "│   "
			}
			detail := fmt.Sprintf("TotalTime: %s (%.2f%%)", formatUs(s.GetSumUs()), share(s.GetSumUs()))
			if len(n.children) > 0 {
				detail += fmt.Sprintf(" [SelfTime: %s (%.2f%%)]", formatUs(n.selfUs), share(n.selfUs))
			}
			line(c, prefix+cont+bar+detail)
			if s.GetCount() > 1 {
				line(c, fmt.Sprintf("%s%s%sCount: %d, MaxTime: %s", prefix, cont, bar, s.GetCount(), formatUs(s.GetMaxUs())))
			}
			where := fmt.Sprintf("Node: %d", s.GetMaxNodeId())
			if s.GetMaxId() != 0 {
				where += fmt.Sprintf(", Id: %d", s.GetMaxId())
			}
			line(c, prefix+cont+bar+where)
			walk(n.children, prefix+cont)
		}
	}
	walk(roots, "")
	return b.String()
}
