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

package rootcoord

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/util/hookutil"
)

// hookutilCipherHelper is the production DatabaseCipherHelper backed by hookutil.
// It stays in internal/rootcoord (depends on internal/util/hookutil) while the
// DatabaseCipherHelper interface can move to the shared pkg module with MetaTable.
type hookutilCipherHelper struct{}

func (hookutilCipherHelper) TidyDBCipherProperties(ezID int64, dbProperties []*commonpb.KeyValuePair) ([]*commonpb.KeyValuePair, error) {
	return hookutil.TidyDBCipherProperties(ezID, dbProperties)
}

func (hookutilCipherHelper) CreateEZByDBProperties(dbProperties []*commonpb.KeyValuePair) error {
	return hookutil.CreateEZByDBProperties(dbProperties)
}

// channelStatsAdapter is the production PChannelStatsManager backed by the
// streamingcoord balancer's static pchannel stats manager. It stays in
// internal/rootcoord (depends on internal/streamingcoord).
type channelStatsAdapter struct{}

func (channelStatsAdapter) Recover(vchannels []string) {
	channel.RecoverPChannelStatsManager(vchannels)
}

func (channelStatsAdapter) AddVChannel(vchannels ...string) {
	channel.StaticPChannelStatsManager.MustGet().AddVChannel(vchannels...)
}

func (channelStatsAdapter) RemoveVChannel(vchannels ...string) {
	channel.StaticPChannelStatsManager.MustGet().RemoveVChannel(vchannels...)
}
