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

package compaction

import (
	"context"
	"path"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

// LoadBM25StatsFromPaths loads BM25 stats from resolved file paths grouped by field ID.
func LoadBM25StatsFromPaths(ctx context.Context, chunkManager storage.ChunkManager, segmentID int64, pathsByField map[int64][]string) (map[int64]*storage.BM25Stats, error) {
	if len(pathsByField) == 0 {
		return nil, nil
	}

	startTs := time.Now()
	log := log.With(zap.Int64("segmentID", segmentID))
	log.Info("begin to reload history BM25 stats")

	fieldList := make([]int64, 0, len(pathsByField))
	fieldOffset := make([]int, 0, len(pathsByField))
	allPaths := make([]string, 0)
	for fieldID, paths := range pathsByField {
		fieldList = append(fieldList, fieldID)
		fieldOffset = append(fieldOffset, len(paths))
		allPaths = append(allPaths, paths...)
	}

	if len(allPaths) == 0 {
		log.Warn("no BM25 stats to load")
		return nil, nil
	}

	values, err := chunkManager.MultiRead(ctx, allPaths)
	if err != nil {
		log.Warn("failed to load BM25 stats files", zap.Error(err))
		return nil, err
	}

	result := make(map[int64]*storage.BM25Stats)
	cnt := 0
	for i, fieldID := range fieldList {
		for offset := 0; offset < fieldOffset[i]; offset++ {
			stats, ok := result[fieldID]
			if !ok {
				stats = storage.NewBM25Stats()
				result[fieldID] = stats
			}
			err := stats.Deserialize(values[cnt+offset])
			if err != nil {
				return nil, err
			}
		}
		cnt += fieldOffset[i]
	}

	log.Info("Successfully load BM25 stats", zap.Any("time", time.Since(startTs)))
	return result, nil
}

// LoadStatsFromPaths loads bloom filter stats from resolved file paths.
// It handles CompoundStatsType detection based on the last path component,
// similar to LoadStats but accepts pre-resolved paths instead of FieldBinlog.
func LoadStatsFromPaths(ctx context.Context, chunkManager storage.ChunkManager, segmentID int64, paths []string) ([]*storage.PkStatistics, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	startTs := time.Now()
	log := log.With(zap.Int64("segmentID", segmentID))
	log.Info("begin to load bloom filter from paths", zap.Int("pathCount", len(paths)))

	// Detect CompoundStatsType
	logType := storage.DefaultStatsType
	for _, p := range paths {
		_, logidx := path.Split(p)
		if logidx == storage.CompoundStatsType.LogIdx() {
			paths = []string{p}
			logType = storage.CompoundStatsType
			break
		}
	}

	values, err := chunkManager.MultiRead(ctx, paths)
	if err != nil {
		log.Warn("failed to load bloom filter files", zap.Error(err))
		return nil, err
	}

	blobs := make([]*storage.Blob, 0, len(values))
	for _, v := range values {
		blobs = append(blobs, &storage.Blob{Value: v})
	}

	var stats []*storage.PrimaryKeyStats
	if logType == storage.CompoundStatsType {
		stats, err = storage.DeserializeStatsList(blobs[0])
	} else {
		stats, err = storage.DeserializeStats(blobs)
	}
	if err != nil {
		log.Warn("failed to deserialize bloom filter stats", zap.Error(err))
		return nil, err
	}

	var size uint
	result := make([]*storage.PkStatistics, 0, len(stats))
	for _, stat := range stats {
		pkStat := &storage.PkStatistics{
			PkFilter: stat.BF,
			MinPK:    stat.MinPk,
			MaxPK:    stat.MaxPk,
		}
		size += stat.BF.Cap()
		result = append(result, pkStat)
	}

	log.Info("Successfully load bloom filter from paths", zap.Any("time", time.Since(startTs)), zap.Uint("size", size))
	return result, nil
}
