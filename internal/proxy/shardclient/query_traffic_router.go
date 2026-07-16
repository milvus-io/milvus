// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/proxy/shardclient/querytraffic"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type queryTrafficConfigProvider interface {
	Enabled() bool
	Rules() string
}

type QueryTrafficLabelProvider interface {
	GetLabels(ctx context.Context, nodeIDs []int64) (querytraffic.Labels, map[int64]querytraffic.Labels, error)
}

type queryTrafficRouter struct {
	configProvider queryTrafficConfigProvider
	labelProvider  QueryTrafficLabelProvider

	mut    sync.Mutex
	raw    string
	policy *querytraffic.Policy
}

func newQueryTrafficRouter(configProvider queryTrafficConfigProvider, labelProvider QueryTrafficLabelProvider) *queryTrafficRouter {
	return &queryTrafficRouter{
		configProvider: configProvider,
		labelProvider:  labelProvider,
	}
}

func (r *queryTrafficRouter) route(ctx context.Context, nodeIDs []int64) ([]WeightedNode, bool, error) {
	if r == nil || r.configProvider == nil || r.labelProvider == nil || !r.configProvider.Enabled() {
		return nil, false, nil
	}
	policy, err := r.getPolicy()
	if err != nil {
		return nil, false, err
	}
	if policy == nil {
		return nil, false, nil
	}

	sourceLabels, nodeLabels, err := r.labelProvider.GetLabels(ctx, nodeIDs)
	if err != nil {
		return nil, false, err
	}
	candidates := make([]querytraffic.Candidate, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		candidates = append(candidates, querytraffic.Candidate{
			NodeID: nodeID,
			Labels: cloneQueryTrafficLabels(nodeLabels[nodeID]),
		})
	}

	routed := policy.Route(cloneQueryTrafficLabels(sourceLabels), candidates)
	if len(routed) == 0 {
		return nil, false, nil
	}
	weightedNodes := make([]WeightedNode, 0, len(routed))
	for _, candidate := range routed {
		weightedNodes = append(weightedNodes, WeightedNode{
			NodeID: candidate.NodeID,
			Weight: candidate.Weight,
		})
	}
	return weightedNodes, true, nil
}

func (r *queryTrafficRouter) getPolicy() (*querytraffic.Policy, error) {
	raw := r.configProvider.Rules()

	r.mut.Lock()
	defer r.mut.Unlock()
	if raw == r.raw {
		return r.policy, nil
	}
	rules, err := querytraffic.ParseRules(raw)
	if err != nil {
		return nil, err
	}
	if len(rules) == 0 {
		r.raw = raw
		r.policy = nil
		return nil, nil
	}
	policy, err := querytraffic.Compile(querytraffic.PolicyConfig{Rules: rules})
	if err != nil {
		return nil, err
	}
	r.raw = raw
	r.policy = policy
	return policy, nil
}

type paramtableQueryTrafficConfig struct{}

func (paramtableQueryTrafficConfig) Enabled() bool {
	return paramtable.Get().ProxyCfg.QueryTrafficRoutingEnabled.GetAsBool()
}

func (paramtableQueryTrafficConfig) Rules() string {
	return paramtable.Get().ProxyCfg.QueryTrafficRoutingRules.GetValue()
}

type sessionQueryTrafficLabelProvider struct {
	session *sessionutil.Session
}

func NewSessionQueryTrafficLabelProvider(session *sessionutil.Session) QueryTrafficLabelProvider {
	return &sessionQueryTrafficLabelProvider{
		session: session,
	}
}

func (p *sessionQueryTrafficLabelProvider) GetLabels(ctx context.Context, nodeIDs []int64) (querytraffic.Labels, map[int64]querytraffic.Labels, error) {
	if p == nil || p.session == nil {
		return nil, nil, nil
	}
	sourceLabels := cloneQueryTrafficLabels(p.session.GetServerLabel())
	sessions, _, err := p.session.GetSessions(ctx, typeutil.QueryNodeRole)
	if err != nil {
		return nil, nil, err
	}

	nodeIDSet := typeutil.NewUniqueSet(nodeIDs...)
	nodeLabels := make(map[int64]querytraffic.Labels, len(nodeIDs))
	for _, session := range sessions {
		if !nodeIDSet.Contain(session.ServerID) {
			continue
		}
		nodeLabels[session.ServerID] = cloneQueryTrafficLabels(session.GetServerLabel())
	}
	return sourceLabels, nodeLabels, nil
}

func cloneQueryTrafficLabels(labels map[string]string) querytraffic.Labels {
	if len(labels) == 0 {
		return nil
	}
	cloned := make(querytraffic.Labels, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}
