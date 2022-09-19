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

package paramtable

import (
	"strings"
	"time"
)

///////////////////////////////////////////////////////////////////////////////
// --- common ---
type commonConfig struct {
	Base *BaseTable

	ClusterPrefix string

	ProxySubName string

	RootCoordTimeTick   string
	RootCoordStatistics string
	RootCoordDml        string
	RootCoordDelta      string
	RootCoordSubName    string

	QueryCoordSearch       string
	QueryCoordSearchResult string
	QueryCoordTimeTick     string
	QueryNodeSubName       string

	DataCoordStatistic   string
	DataCoordTimeTick    string
	DataCoordSegmentInfo string
	DataCoordSubName     string
	DataNodeSubName      string

	DefaultPartitionName string
	DefaultIndexName     string
	RetentionDuration    int64
	EntityExpirationTTL  time.Duration

	IndexSliceSize int64
	GracefulTime   int64

	StorageType string
	SimdType    string

	AuthorizationEnabled bool

	ClusterName string
}

func (p *commonConfig) init(base *BaseTable) {
	p.Base = base

	// must init cluster prefix first
	p.initClusterPrefix()
	p.initProxySubName()

	p.initRootCoordTimeTick()
	p.initRootCoordStatistics()
	p.initRootCoordDml()
	p.initRootCoordDelta()
	p.initRootCoordSubName()

	p.initQueryCoordTimeTick()
	p.initQueryNodeSubName()

	p.initDataCoordStatistic()
	p.initDataCoordTimeTick()
	p.initDataCoordSegmentInfo()
	p.initDataCoordSubName()
	p.initDataNodeSubName()

	p.initDefaultPartitionName()
	p.initDefaultIndexName()
	p.initRetentionDuration()
	p.initEntityExpiration()

	p.initSimdType()
	p.initIndexSliceSize()
	p.initGracefulTime()
	p.initStorageType()

	p.initEnableAuthorization()

	p.initClusterName()
}

func (p *commonConfig) initClusterPrefix() {
	keys := []string{
		"msgChannel.chanNamePrefix.cluster",
		"common.chanNamePrefix.cluster",
	}
	str, err := p.Base.LoadWithPriority(keys)
	if err != nil {
		panic(err)
	}
	p.ClusterPrefix = str
}

func (p *commonConfig) initChanNamePrefix(keys []string) string {
	value, err := p.Base.LoadWithPriority(keys)
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterPrefix, value}
	return strings.Join(s, "-")
}

// --- proxy ---
func (p *commonConfig) initProxySubName() {
	keys := []string{
		"msgChannel.subNamePrefix.proxySubNamePrefix",
		"common.subNamePrefix.proxySubNamePrefix",
	}
	p.ProxySubName = p.initChanNamePrefix(keys)
}

// --- rootcoord ---
// Deprecate
func (p *commonConfig) initRootCoordTimeTick() {
	keys := []string{
		"msgChannel.chanNamePrefix.rootCoordTimeTick",
		"common.chanNamePrefix.rootCoordTimeTick",
	}
	p.RootCoordTimeTick = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordStatistics() {
	keys := []string{
		"msgChannel.chanNamePrefix.rootCoordStatistics",
		"common.chanNamePrefix.rootCoordStatistics",
	}
	p.RootCoordStatistics = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordDml() {
	keys := []string{
		"msgChannel.chanNamePrefix.rootCoordDml",
		"common.chanNamePrefix.rootCoordDml",
	}
	p.RootCoordDml = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordDelta() {
	keys := []string{
		"msgChannel.chanNamePrefix.rootCoordDelta",
		"common.chanNamePrefix.rootCoordDelta",
	}
	p.RootCoordDelta = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordSubName() {
	keys := []string{
		"msgChannel.subNamePrefix.rootCoordSubNamePrefix",
		"common.subNamePrefix.rootCoordSubNamePrefix",
	}
	p.RootCoordSubName = p.initChanNamePrefix(keys)
}

// Deprecate
func (p *commonConfig) initQueryCoordTimeTick() {
	keys := []string{
		"msgChannel.chanNamePrefix.queryTimeTick",
		"common.chanNamePrefix.queryTimeTick",
	}
	p.QueryCoordTimeTick = p.initChanNamePrefix(keys)
}

// --- querynode ---
func (p *commonConfig) initQueryNodeSubName() {
	keys := []string{
		"msgChannel.subNamePrefix.queryNodeSubNamePrefix",
		"common.subNamePrefix.queryNodeSubNamePrefix",
	}
	p.QueryNodeSubName = p.initChanNamePrefix(keys)
}

// --- datacoord ---
func (p *commonConfig) initDataCoordStatistic() {
	keys := []string{
		"msgChannel.chanNamePrefix.dataCoordStatistic",
		"common.chanNamePrefix.dataCoordStatistic",
	}
	p.DataCoordStatistic = p.initChanNamePrefix(keys)
}

// Deprecate
func (p *commonConfig) initDataCoordTimeTick() {
	keys := []string{
		"msgChannel.chanNamePrefix.dataCoordTimeTick",
		"common.chanNamePrefix.dataCoordTimeTick",
	}
	p.DataCoordTimeTick = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDataCoordSegmentInfo() {
	keys := []string{
		"msgChannel.chanNamePrefix.dataCoordSegmentInfo",
		"common.chanNamePrefix.dataCoordSegmentInfo",
	}
	p.DataCoordSegmentInfo = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDataCoordSubName() {
	keys := []string{
		"msgChannel.subNamePrefix.dataCoordSubNamePrefix",
		"common.subNamePrefix.dataCoordSubNamePrefix",
	}
	p.DataCoordSubName = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDataNodeSubName() {
	keys := []string{
		"msgChannel.subNamePrefix.dataNodeSubNamePrefix",
		"common.subNamePrefix.dataNodeSubNamePrefix",
	}
	p.DataNodeSubName = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDefaultPartitionName() {
	p.DefaultPartitionName = p.Base.LoadWithDefault("common.defaultPartitionName", "_default")
}

func (p *commonConfig) initDefaultIndexName() {
	p.DefaultIndexName = p.Base.LoadWithDefault("common.defaultIndexName", "_default_idx")
}

func (p *commonConfig) initRetentionDuration() {
	p.RetentionDuration = p.Base.ParseInt64WithDefault("common.retentionDuration", DefaultRetentionDuration)
}

func (p *commonConfig) initEntityExpiration() {
	ttl := p.Base.ParseInt64WithDefault("common.entityExpiration", -1)
	if ttl < 0 {
		p.EntityExpirationTTL = -1
		return
	}

	// make sure ttl is larger than retention duration to ensure time travel works
	if ttl > p.RetentionDuration {
		p.EntityExpirationTTL = time.Duration(ttl) * time.Second
	} else {
		p.EntityExpirationTTL = time.Duration(p.RetentionDuration) * time.Second
	}
}

func (p *commonConfig) initSimdType() {
	keys := []string{
		"common.simdType",
		"knowhere.simdType",
	}
	p.SimdType = p.Base.LoadWithDefault2(keys, "auto")
}

func (p *commonConfig) initIndexSliceSize() {
	p.IndexSliceSize = p.Base.ParseInt64WithDefault("common.indexSliceSize", DefaultIndexSliceSize)
}

func (p *commonConfig) initGracefulTime() {
	p.GracefulTime = p.Base.ParseInt64WithDefault("common.gracefulTime", DefaultGracefulTime)
}

func (p *commonConfig) initStorageType() {
	p.StorageType = p.Base.LoadWithDefault("common.storageType", "minio")
}

func (p *commonConfig) initEnableAuthorization() {
	p.AuthorizationEnabled = p.Base.ParseBool("common.security.authorizationEnabled", false)
}

func (p *commonConfig) initClusterName() {
	p.ClusterName = p.Base.LoadWithDefault("common.cluster.name", "")
}
