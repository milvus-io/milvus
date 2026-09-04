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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	flushio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/textindex"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// writeCompactionTextTerms rebuilds the FST from the newly written segment,
// rather than copying source dictionaries. Reading the output makes the
// artifact follow the exact post-delete/post-TTL row set and the writer's
// output-segment rotation. StorageV3 uses the LOB-aware reader so TEXT values
// are analyzed as UTF-8 instead of encoded references.
func writeCompactionTextTerms(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	segment *datapb.CompactionSegment,
	collectionID, partitionID int64,
	binlogIO flushio.BinlogIO,
	logAllocator allocator.Interface,
	params compaction.Params,
) error {
	if segment.GetNumOfRows() == 0 {
		return nil
	}
	var downloader func(context.Context, []string) ([][]byte, error)
	if binlogIO != nil {
		downloader = binlogIO.Download
	}
	textTerms, err := function.CollectSegmentTextTerms(ctx, schema, function.SegmentTextTermSource{
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		SegmentID:      segment.GetSegmentID(),
		StorageVersion: segment.GetStorageVersion(),
		InsertLogs:     segment.GetInsertLogs(),
		ManifestPath:   segment.GetManifest(),
		StorageConfig:  params.StorageConfig,
		Downloader:     downloader,
	})
	if err != nil {
		return err
	}
	if len(textTerms.Fields) == 0 {
		return nil
	}
	if logAllocator == nil {
		return merr.WrapErrServiceInternalMsg("text term FST log allocator is nil")
	}
	var fstSize int64
	if segment.GetStorageVersion() < storage.StorageV3 {
		if binlogIO == nil {
			return merr.WrapErrServiceInternalMsg("text term FST binlog IO is nil")
		}
		logs, size, err := textindex.WriteFieldBinlogs(ctx, binlogIO.Upload,
			params.StorageConfig.GetRootPath(), collectionID, partitionID, segment.GetSegmentID(),
			logAllocator, textTerms.Fields, textTerms.CoverageTimestamp)
		if err != nil {
			return err
		}
		segment.TextLogV2 = logs
		fstSize = size
	} else {
		entries, size, err := textindex.BuildReplacementManifestEntries(segment.GetManifest(), params.StorageConfig,
			logAllocator, textTerms.Fields, textTerms.CoverageTimestamp)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		basePath, version, err := packed.UnmarshalManifestPath(segment.GetManifest())
		if err != nil {
			return err
		}
		manifest, err := packed.CommitManifestUpdates(basePath, version, params.StorageConfig,
			&packed.ManifestUpdates{Stats: entries})
		if err != nil {
			return merr.Wrap(err, "commit compaction text term FST manifest")
		}
		segment.Manifest = manifest
		fstSize = size
	}
	if segment.Stats == nil {
		segment.Stats = &datapb.Statistics{}
	}
	segment.Stats.StatsBinlogSize += fstSize
	return nil
}

func writeCompactionTextTermsForSegments(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	segments []*datapb.CompactionSegment,
	collectionID, partitionID int64,
	binlogIO flushio.BinlogIO,
	logAllocator allocator.Interface,
	params compaction.Params,
) error {
	for _, segment := range segments {
		if err := writeCompactionTextTerms(ctx, schema, segment, collectionID, partitionID,
			binlogIO, logAllocator, params); err != nil {
			return err
		}
	}
	return nil
}
