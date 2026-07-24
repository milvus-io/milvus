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
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// buildTextIndexArgs bundles per-call inputs for buildTextIndexesForSegment.
// taskID is recorded as the BuildID on each produced TextIndexStats; for both
// sort_compaction and mix_compaction this is the compaction plan ID.
type buildTextIndexArgs struct {
	plan             *datapb.CompactionPlan
	compactionParams compaction.Params
	collectionID     int64
	partitionID      int64
	segmentID        int64
	taskID           int64
	storageVersion   int64
	manifest         string
	insertBinlogs    []*datapb.FieldBinlog
}

// buildTextIndexesForSegment creates text indexes for all enable_match fields
// on a single compaction output and returns the per-field TextIndexStats. The
// caller is responsible for assigning the result to the output segment's
// TextStatsLogs.
func buildTextIndexesForSegment(ctx context.Context, args buildTextIndexArgs) (map[int64]*datapb.TextIndexStats, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", args.collectionID),
		zap.Int64("partitionID", args.partitionID),
		zap.Int64("segmentID", args.segmentID),
	)

	fieldBinlogs := lo.GroupBy(args.insertBinlogs, func(binlog *datapb.FieldBinlog) int64 {
		return binlog.GetFieldID()
	})

	getInsertFiles := func(fieldID int64) ([]string, error) {
		if args.storageVersion == storage.StorageV2 {
			return []string{}, nil
		}
		binlogs, ok := fieldBinlogs[fieldID]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("field binlog not found for field %d", fieldID)
		}
		result := make([]string, 0, len(binlogs))
		for _, binlog := range binlogs {
			for _, file := range binlog.GetBinlogs() {
				result = append(result, metautil.BuildInsertLogPath(args.compactionParams.StorageConfig.GetRootPath(),
					args.collectionID, args.partitionID, args.segmentID, fieldID, file.GetLogID()))
			}
		}
		return result, nil
	}

	newStorageConfig, err := util.ParseStorageConfig(args.compactionParams.StorageConfig)
	if err != nil {
		return nil, err
	}
	pluginContext, err := hookutil.GetCPluginContext(args.plan.GetPluginContext(), args.collectionID)
	if err != nil {
		return nil, err
	}

	var (
		mu            sync.Mutex
		textIndexLogs = make(map[int64]*datapb.TextIndexStats)
	)

	eg, egCtx := errgroup.WithContext(ctx)

	for _, field := range args.plan.GetSchema().GetFields() {
		field := field
		h := typeutil.CreateFieldSchemaHelper(field)
		if !h.EnableMatch() {
			continue
		}
		log.Info("field enable match, ready to create text index", zap.Int64("field id", field.GetFieldID()))

		eg.Go(func() error {
			files, err := getInsertFiles(field.GetFieldID())
			if err != nil {
				return err
			}

			buildIndexParams := &indexcgopb.BuildIndexInfo{
				BuildID:                   args.taskID,
				CollectionID:              args.collectionID,
				PartitionID:               args.partitionID,
				SegmentID:                 args.segmentID,
				IndexVersion:              0, // always zero
				InsertFiles:               files,
				FieldSchema:               field,
				StorageConfig:             newStorageConfig,
				CurrentScalarIndexVersion: common.ClampScalarIndexVersion(args.plan.GetCurrentScalarIndexVersion()),
				StorageVersion:            args.storageVersion,
				Manifest:                  args.manifest,
			}
			if pluginContext != nil {
				buildIndexParams.StoragePluginContext = pluginContext
			}

			if args.storageVersion == storage.StorageV2 {
				buildIndexParams.SegmentInsertFiles = util.GetSegmentInsertFiles(
					args.insertBinlogs,
					args.compactionParams.StorageConfig,
					args.collectionID,
					args.partitionID,
					args.segmentID)
			}
			uploaded, err := indexcgowrapper.CreateTextIndex(egCtx, buildIndexParams)
			if err != nil {
				return err
			}

			mu.Lock()
			totalSize := lo.SumBy(lo.Values(uploaded), func(fileSize int64) int64 { return fileSize })
			textIndexLogs[field.GetFieldID()] = &datapb.TextIndexStats{
				FieldID:                   field.GetFieldID(),
				Version:                   0,
				BuildID:                   args.taskID,
				Files:                     lo.Keys(uploaded),
				LogSize:                   totalSize,
				MemorySize:                totalSize,
				CurrentScalarIndexVersion: common.ClampScalarIndexVersion(args.plan.GetCurrentScalarIndexVersion()),
			}
			mu.Unlock()

			log.Info("field enable match, create text index done",
				zap.Int64("segmentID", args.segmentID),
				zap.Int64("field id", field.GetFieldID()),
				zap.Strings("files", lo.Keys(uploaded)),
			)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return textIndexLogs, nil
}
