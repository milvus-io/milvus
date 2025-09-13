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

package replicateutil

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type Role int

const (
	RolePrimary Role = iota
	RoleSecondary
)

var ErrWrongConfiguration = errors.New("wrong replicate configuration")

func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "primary"
	case RoleSecondary:
		return "secondary"
	default:
		panic(r)
	}
}

// MustNewConfigHelper creates a new graph from the replicate configuration.
func MustNewConfigHelper(currentClusterID string, cfg *commonpb.ReplicateConfiguration) *ConfigHelper {
	g, err := NewConfigHelper(currentClusterID, cfg)
	if err != nil {
		panic(err)
	}
	return g
}

// NewConfigHelper creates a new graph from the replicate configuration.
func NewConfigHelper(currentClusterID string, cfg *commonpb.ReplicateConfiguration) (*ConfigHelper, error) {
	if cfg == nil {
		return nil, nil
	}
	h := &ConfigHelper{}
	vs := make(map[string]*MilvusCluster)
	for _, cluster := range cfg.GetClusters() {
		vs[cluster.GetClusterId()] = &MilvusCluster{
			h:             h,
			MilvusCluster: cluster,
			idxMap:        make(map[string]int),
			role:          RolePrimary,
			source:        "",
			targets:       typeutil.NewSet[string](),
		}
		for i, pchannel := range cluster.Pchannels {
			vs[cluster.GetClusterId()].idxMap[pchannel] = i
		}
	}
	for _, topology := range cfg.GetCrossClusterTopology() {
		if _, ok := vs[topology.SourceClusterId]; !ok {
			return nil, ErrWrongConfiguration
		}
		if _, ok := vs[topology.TargetClusterId]; !ok {
			return nil, ErrWrongConfiguration
		}
		if vs[topology.SourceClusterId].targets.Contain(topology.TargetClusterId) {
			return nil, ErrWrongConfiguration
		}
		if vs[topology.TargetClusterId].source != "" {
			return nil, ErrWrongConfiguration
		}
		vs[topology.TargetClusterId].source = topology.SourceClusterId
		vs[topology.TargetClusterId].role = RoleSecondary
		vs[topology.SourceClusterId].targets.Insert(topology.TargetClusterId)
	}
	primaryCount := 0
	for _, vertice := range vs {
		if vertice.role == RolePrimary {
			primaryCount++
		}
	}
	if primaryCount != 1 {
		return nil, errors.Wrap(ErrWrongConfiguration, "primary count is not 1")
	}
	if _, ok := vs[currentClusterID]; !ok {
		return nil, errors.Wrap(ErrWrongConfiguration, fmt.Sprintf("current cluster %s not found", currentClusterID))
	}
	pchannels := len(vs[currentClusterID].Pchannels)
	for _, vertice := range vs {
		if len(vertice.Pchannels) != pchannels {
			return nil, errors.Wrap(ErrWrongConfiguration, fmt.Sprintf("pchannel count is not equal for cluster %s", vertice.GetClusterId()))
		}
	}
	h.currentClusterID = currentClusterID
	h.cfg = cfg
	h.vs = vs
	return h, nil
}

// ConfigHelper describes the replicate topology.
type ConfigHelper struct {
	currentClusterID string
	cfg              *commonpb.ReplicateConfiguration
	vs               map[string]*MilvusCluster
}

// GetReplicateConfiguration returns the replicate configuration of the graph.
func (g *ConfigHelper) GetReplicateConfiguration() *commonpb.ReplicateConfiguration {
	return g.cfg
}

// GetCurrentCluster returns the current cluster id.
func (g *ConfigHelper) GetCurrentCluster() *MilvusCluster {
	return g.vs[g.currentClusterID]
}

// GetCluster returns the cluster from the graph.
func (g *ConfigHelper) GetCluster(clusterID string) *MilvusCluster {
	return g.vs[clusterID]
}

// MustGetCluster returns the cluster from the graph.
func (g *ConfigHelper) MustGetCluster(clusterID string) *MilvusCluster {
	vertice, ok := g.vs[clusterID]
	if !ok {
		panic(fmt.Sprintf("cluster %s not found", clusterID))
	}
	return vertice
}

// MilvusCluster describes the replicate topology.
type MilvusCluster struct {
	*commonpb.MilvusCluster
	h       *ConfigHelper
	role    Role
	idxMap  map[string]int
	source  string
	targets typeutil.Set[string]
}

// Role returns the role of the milvus cluster.
func (v *MilvusCluster) Role() Role {
	return v.role
}

// SourceCluster returns the source cluster of the milvus.
// return nil if the milvus cluster is a primary node.
func (v *MilvusCluster) SourceCluster() *MilvusCluster {
	if v.role == RolePrimary {
		return nil
	}
	return v.h.vs[v.source]
}

// TargetClusters returns the target clusters of the milvus.
func (v *MilvusCluster) TargetClusters() []*MilvusCluster {
	targets := make([]*MilvusCluster, 0, len(v.targets))
	for target := range v.targets {
		targets = append(targets, v.h.vs[target])
	}
	return targets
}

// TargetCluster returns the target cluster of the milvus.
func (v *MilvusCluster) TargetCluster(targetClusterID string) *MilvusCluster {
	if !v.targets.Contain(targetClusterID) {
		return nil
	}
	return v.h.vs[targetClusterID]
}

// MustGetSourceChannel returns the source channel by the current cluster channel.
func (v *MilvusCluster) MustGetSourceChannel(pchannel string) string {
	source := v.SourceCluster()
	if source == nil {
		panic(fmt.Sprintf("source cluster not found for milvus cluster %s", v.GetClusterId()))
	}
	idx, ok := v.idxMap[pchannel]
	if !ok {
		panic(fmt.Sprintf("channel of current cluster not found for pchannel: %s", pchannel))
	}
	return source.Pchannels[idx]
}

// GetTargetChannel returns the target channel of the current cluster.
func (v *MilvusCluster) GetTargetChannel(currentClusterPChannel string, targetClusterID string) (string, error) {
	if !v.targets.Contain(targetClusterID) {
		return "", errors.Errorf("target cluster %s not found, current cluster is %s", targetClusterID, v.GetClusterId())
	}
	idx, ok := v.idxMap[currentClusterPChannel]
	if !ok {
		return "", errors.Errorf("current cluster pchannel %s not found in the graph", currentClusterPChannel)
	}
	target := v.h.vs[targetClusterID]
	return target.Pchannels[idx], nil
}
