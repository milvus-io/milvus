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

package util

import (
	"context"
	"path"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func LoadStats(ctx context.Context, chunkManager storage.ChunkManager, schema *schemapb.CollectionSchema, segmentID int64, statsBinlogs []*datapb.FieldBinlog) ([]*storage.PkStatistics, error) {
	startTs := time.Now()
	log := log.With(zap.Int64("segmentID", segmentID))
	log.Info("begin to init pk bloom filter", zap.Int("statsBinLogsLen", len(statsBinlogs)))

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	// filter stats binlog files which is pk field stats log
	bloomFilterFiles := []string{}
	logType := storage.DefaultStatsType

	for _, binlog := range statsBinlogs {
		if binlog.FieldID != pkField.GetFieldID() {
			continue
		}
	Loop:
		for _, log := range binlog.GetBinlogs() {
			_, logidx := path.Split(log.GetLogPath())
			// if special status log exist
			// only load one file
			switch logidx {
			case storage.CompoundStatsType.LogIdx():
				bloomFilterFiles = []string{log.GetLogPath()}
				logType = storage.CompoundStatsType
				break Loop
			default:
				bloomFilterFiles = append(bloomFilterFiles, log.GetLogPath())
			}
		}
	}

	// no stats log to parse, initialize a new BF
	if len(bloomFilterFiles) == 0 {
		log.Warn("no stats files to load")
		return nil, nil
	}

	// read historical PK filter
	values, err := chunkManager.MultiRead(ctx, bloomFilterFiles)
	if err != nil {
		log.Warn("failed to load bloom filter files", zap.Error(err))
		return nil, err
	}
	blobs := make([]*storage.Blob, 0)
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &storage.Blob{Value: values[i]})
	}

	var stats []*storage.PrimaryKeyStats
	if logType == storage.CompoundStatsType {
		stats, err = storage.DeserializeStatsList(blobs[0])
		if err != nil {
			log.Warn("failed to deserialize stats list", zap.Error(err))
			return nil, err
		}
	} else {
		stats, err = storage.DeserializeStats(blobs)
		if err != nil {
			log.Warn("failed to deserialize stats", zap.Error(err))
			return nil, err
		}
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

	log.Info("Successfully load pk stats", zap.Any("time", time.Since(startTs)), zap.Uint("size", size))
	return result, nil
}

func LoadStatsV2(storageCache *metacache.StorageV2Cache, segment *datapb.SegmentInfo, schema *schemapb.CollectionSchema) ([]*storage.PkStatistics, error) {
	space, err := storageCache.GetOrCreateSpace(segment.ID, syncmgr.SpaceCreatorFunc(segment.ID, schema, storageCache.ArrowSchema()))
	if err != nil {
		return nil, err
	}

	getResult := func(stats []*storage.PrimaryKeyStats) []*storage.PkStatistics {
		result := make([]*storage.PkStatistics, 0, len(stats))
		for _, stat := range stats {
			pkStat := &storage.PkStatistics{
				PkFilter: stat.BF,
				MinPK:    stat.MinPk,
				MaxPK:    stat.MaxPk,
			}
			result = append(result, pkStat)
		}
		return result
	}

	blobs := space.StatisticsBlobs()
	deserBlobs := make([]*storage.Blob, 0)
	for _, b := range blobs {
		if b.Name == storage.CompoundStatsType.LogIdx() {
			blobData := make([]byte, b.Size)
			_, err = space.ReadBlob(b.Name, blobData)
			if err != nil {
				return nil, err
			}
			stats, err := storage.DeserializeStatsList(&storage.Blob{Value: blobData})
			if err != nil {
				return nil, err
			}
			return getResult(stats), nil
		}
	}

	for _, b := range blobs {
		blobData := make([]byte, b.Size)
		_, err = space.ReadBlob(b.Name, blobData)
		if err != nil {
			return nil, err
		}
		deserBlobs = append(deserBlobs, &storage.Blob{Value: blobData})
	}
	stats, err := storage.DeserializeStats(deserBlobs)
	if err != nil {
		return nil, err
	}
	return getResult(stats), nil
}
