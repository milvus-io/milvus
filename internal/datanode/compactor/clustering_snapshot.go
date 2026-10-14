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

package compactor

import (
	"context"
	"path"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clusteringindex"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

func (t *clusteringCompactionTask) buildClusteringSnapshot(ctx context.Context, delta *storage.PartitionStatsSnapshot) (*storage.PartitionStatsSnapshot, error) {
	root := t.compactionParams.StorageConfig.GetRootPath()
	prefix := path.Join(root, common.PartitionStatsPath, metautil.JoinIDPath(t.collectionID, t.partitionID), t.plan.GetChannel())
	read := func(key string) ([]byte, error) {
		blobs, err := t.binlogIO.Download(ctx, []string{key})
		if err != nil {
			return nil, err
		}
		if len(blobs) != 1 {
			return nil, merr.WrapErrDataIntegrityMsg("missing clustering snapshot artifact")
		}
		return blobs[0], nil
	}
	var base *storage.PartitionStatsSnapshot
	var baseCentroids *clusteringpb.ClusteringCentroidsStats
	if version := t.plan.GetBasePartitionStatsVersion(); version > 0 {
		blob, err := read(path.Join(prefix, strconv.FormatInt(version, 10)))
		if err != nil {
			return nil, err
		}
		base, err = storage.DeserializePartitionsStatsSnapshot(blob)
		if err != nil {
			return nil, err
		}
		if base.Version != version {
			return nil, merr.WrapErrDataIntegrityMsg("base snapshot version mismatch")
		}
		if base.ClusteringStats != nil && base.ClusteringStats.GetNumClusters() > 0 {
			blob, err = read(base.ClusteringStats.GetClustersFile())
			if err != nil {
				return nil, err
			}
			baseCentroids = &clusteringpb.ClusteringCentroidsStats{}
			if err := proto.Unmarshal(blob, baseCentroids); err != nil {
				return nil, merr.WrapErrDataIntegrity(err, "decode base cluster table")
			}
		}
	}
	inputs := make([]int64, 0, len(t.plan.GetSegmentBinlogs()))
	for _, segment := range t.plan.GetSegmentBinlogs() {
		inputs = append(inputs, segment.GetSegmentID())
	}
	draft, err := storage.MergeClusteringSnapshot(base, baseCentroids, inputs, delta.SegmentStats, delta.ClusteringStats.GetGroups(), t.vectorCentroids, t.clusteringKeyField, t.plan.GetPlanID())
	if err != nil {
		return nil, err
	}
	if draft.Snapshot.ClusteringStats.GetNumClusters() == 0 {
		return draft.Finalize("", nil)
	}
	options := t.compactionParams.ClusteringPlan
	if options == nil || len(options.IndexParams) == 0 || options.IndexEngineVersion <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("missing frozen snapshot index parameters")
	}
	config, err := util.ParseStorageConfig(t.compactionParams.StorageConfig)
	if err != nil {
		return nil, err
	}
	plugin, err := hookutil.GetCPluginContext(t.plan.GetPluginContext(), t.collectionID)
	if err != nil {
		return nil, err
	}
	key := path.Join(prefix, strconv.FormatInt(t.plan.GetPlanID(), 10))
	clustersKey := key + ".clusters"
	table, err := proto.Marshal(draft.Centroids)
	if err != nil {
		return nil, err
	}
	if err := t.binlogIO.Upload(ctx, map[string][]byte{clustersKey: table}); err != nil {
		return nil, err
	}
	index, err := clusteringindex.Build(ctx, &indexcgopb.BuildIndexInfo{
		CollectionID: t.collectionID, PartitionID: t.partitionID, IndexVersion: t.plan.GetPlanID(),
		FieldSchema: t.clusteringKeyField, Dim: draft.Snapshot.ClusteringStats.GetDim(), NumRows: int64(draft.Snapshot.ClusteringStats.GetNumClusters()),
		IndexParams: funcutil.Map2KeyValuePair(options.IndexParams), CurrentIndexVersion: options.IndexEngineVersion,
		StorageConfig: config, StoragePluginContext: plugin,
	}, draft.Centroids, key+".clustering_index")
	if err != nil {
		return nil, err
	}
	return draft.Finalize(clustersKey, index)
}
