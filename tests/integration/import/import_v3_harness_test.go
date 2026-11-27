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

package importv2

import (
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

// configureImportV3 applies the Import V3 rollout gate and its companion
// settings to a suite's *startup* configuration. It must be called before the
// embedded MiniClusterSuite.SetupSuite starts the cluster, because the gate and
// the storage layout are resolved when the components boot.
//
// The V2 suites in this package all run with the gate off; every V3 suite routes
// its import through these settings.
func configureImportV3(cluster *integration.MiniClusterSuite) {
	configureImportV3Gate(cluster)
	// Ordinary V3 import writes its formal segments through the LOON (Milvus
	// Storage V3) writer.
	cluster.WithMilvusConfig(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
}

// configureImportV3Gate enables the gate and its scheduling companions but
// leaves the storage format at its default (Storage V2). The backup/binlog suite
// uses this because its source is a collection in the same cluster: the legacy
// per-field binlog reader only understands Storage V2 sources, so the whole
// cluster must stay on Storage V2 for the source to be readable back.
func configureImportV3Gate(cluster *integration.MiniClusterSuite) {
	cluster.WithMilvusConfig(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, "4")
	cluster.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableImportV3.Key, "true")
	cluster.WithMilvusConfig(paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.Key, "true")
	// V3 generates deterministic PK/RowID for ranged (ordinary AutoID) imports
	// by broadcasting the per-file ID range after the count-only preimport.
	cluster.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportEnableIDRangeMsg.Key, "true")
	// Small fixtures should exercise every stage without waiting for the
	// production scheduling intervals.
	cluster.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportCheckIntervalHigh.Key, "0.1")
	cluster.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportScheduleInterval.Key, "0.1")
}
