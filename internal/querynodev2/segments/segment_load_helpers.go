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

package segments

/*
#cgo pkg-config: milvus_core

#include "segcore/load_index_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/logutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// These load operations accept explicit metadata and ownership, and are shared
// by the legacy loader and QueryView resources without consulting registries.

type DeltaLoadTarget interface {
	ID() int64
	LastDeltaTimestamp() uint64
	LoadDeltaData(context.Context, *storage.DeltaData) error
}

func LoadSegmentBloomFilters(ctx context.Context, schema *schemapb.CollectionSchema, collectionID int64, cm storage.ChunkManager, infos ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error) {
	segmentNum := len(infos)
	if segmentNum == 0 {
		mlog.Info(ctx, "no segment to load")
		return nil, nil
	}

	// Phase 1: always create metadata-only stubs (segmentID / partitionID / type).
	// This gives callers valid candidates even when BF data is not loaded,
	// so partition filtering and type-based delete-scope logic never need nil guards.
	bfSets := make([]*pkoracle.BloomFilterSet, segmentNum)
	for i, info := range infos {
		bfSets[i] = pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
	}

	isExternalCollection := typeutil.IsExternalCollection(schema)
	isMilvusTableRealPK := typeutil.NewStorageColumnResolver(schema).IsMilvusTable() &&
		HasExternalPrimaryKey(schema)

	// Phase 2: load BF stats into the stubs. Milvus-table real-PK correctness
	// depends on source bloom filters, so that path ignores the global BF
	// disable switch; other collections keep the historical metadata-only
	// behavior when BloomFilterEnabled=false.
	if !paramtable.Get().CommonCfg.BloomFilterEnabled.GetAsBool() && !isMilvusTableRealPK {
		mlog.Info(ctx, "bloom filter disabled: returning metadata-only stubs")
		return bfSets, nil
	}

	// Virtual-PK external collections use ExternalSegmentCandidate and have no
	// reusable source-side PK stats.
	if isExternalCollection && !isMilvusTableRealPK {
		return bfSets, nil
	}

	pkField := GetPkField(schema)
	pkFieldID := pkField.GetFieldID()

	// Calculate total memory size needed for bloom filters (PK stats)
	var totalMemorySize int64
	for _, info := range infos {
		memSize, _ := packed.NewStatsResolverFromLoadInfo(info).BloomFilterMemorySize(pkFieldID)
		totalMemorySize += memSize
	}

	// Reserve memory resource if tiered eviction is enabled
	if paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() && totalMemorySize > 0 {
		if ok := C.TryReserveLoadingResourceWithTimeout(C.CResourceUsage{
			// double loading memory size for bloom filters to avoid OOM during loading
			memory_bytes: C.int64_t(totalMemorySize * 2),
			disk_bytes:   C.int64_t(0),
		}, 1000); !ok {
			return nil, merr.WrapErrSegmentRequestResourceFailed("memory",
				fmt.Sprintf("failed to reserve loading resource for bloom filters, totalMemorySize = %v MB",
					logutil.ToMB(float64(totalMemorySize))))
		}
		mlog.Debug(ctx, "reserved loading resource for bloom filters", mlog.Float64("totalMemorySizeMB", logutil.ToMB(float64(totalMemorySize))))
	}

	defer func() {
		if paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() && totalMemorySize > 0 {
			C.ReleaseLoadingResource(C.CResourceUsage{
				memory_bytes: C.int64_t(totalMemorySize * 2),
				disk_bytes:   C.int64_t(0),
			})
			mlog.Debug(ctx, "released loading resource for bloom filters", mlog.Float64("totalMemorySizeMB", logutil.ToMB(float64(totalMemorySize))))
		}
	}()

	mlog.Debug(ctx, "start loading remote...", mlog.Int("segmentNum", segmentNum))

	loadRemoteFunc := func(idx int) error {
		loadInfo := infos[idx]
		bfs := bfSets[idx]

		mlog.Debug(ctx, "loading bloom filter for remote...")
		pkStatsBinlogs, err := packed.NewStatsResolverFromLoadInfo(loadInfo).BloomFilterPaths(pkFieldID)
		if err != nil {
			return err
		}
		err = loadBloomFilterWithDownloader(ctx, bfs.ID(), bfs, pkStatsBinlogs, bloomFilterDownloader(schema, collectionID, cm, isMilvusTableRealPK))
		if err != nil {
			mlog.Warn(ctx, "load remote segment bloom filter failed",
				mlog.Int64("partitionID", bfs.Partition()),
				mlog.Int64("segmentID", bfs.ID()),
				mlog.Err(err),
			)
			return err
		}
		if isMilvusTableRealPK && !bfs.PkCandidateExist() {
			return merr.WrapErrServiceInternalMsg("milvus-table real-PK segment missing bloom filter stats")
		}
		return nil
	}

	err := funcutil.ProcessFuncParallel(segmentNum, segmentNum, loadRemoteFunc, "loadRemoteFunc")
	if err != nil {
		// no partial success here
		mlog.Warn(ctx, "failed to load remote segment", mlog.Err(err))
		return nil, err
	}

	// Charge loaded resource for bloom filters
	for _, bfs := range bfSets {
		bfs.Charge()
	}

	return bfSets, nil
}

func bloomFilterDownloader(
	schema *schemapb.CollectionSchema, collectionID int64, cm storage.ChunkManager,
	useExternalSpec bool,
) func(context.Context, []string) ([][]byte, error) {
	if !useExternalSpec {
		return cm.MultiRead
	}
	extfs := packed.ExternalSpecContext{
		CollectionID: collectionID,
		Source:       schema.GetExternalSource(),
		Spec:         schema.GetExternalSpec(),
	}
	return func(ctx context.Context, paths []string) ([][]byte, error) {
		return readExternalFiles(ctx, createStorageConfig(), extfs, paths)
	}
}

func loadBloomFilterWithDownloader(
	ctx context.Context,
	segmentID int64,
	bfs *pkoracle.BloomFilterSet,
	binlogPaths []string,
	downloader func(context.Context, []string) ([][]byte, error),
) error {
	if len(binlogPaths) == 0 {
		mlog.Info(ctx, "there are no stats logs saved with segment")
		return nil
	}

	startTs := time.Now()
	values, err := downloader(ctx, binlogPaths)
	if err != nil {
		return err
	}
	blobs := make([]*storage.Blob, len(values))
	for i := range values {
		blobs[i] = &storage.Blob{Value: values[i]}
	}

	stats, err := storage.DeserializeBloomFilterStats(binlogPaths, blobs)
	if err != nil {
		mlog.Warn(ctx, "failed to deserialize bloom filter stats", mlog.Err(err))
		return err
	}

	var size uint
	for _, stat := range stats {
		pkStat := &storage.PkStatistics{
			PkFilter: stat.BF,
			MinPK:    stat.MinPk,
			MaxPK:    stat.MaxPk,
		}
		size += stat.BF.Cap()
		bfs.AddHistoricalStats(pkStat)
	}
	mlog.Debug(ctx, "Successfully load pk stats", mlog.Duration("time", time.Since(startTs)), mlog.Uint("size", size))
	return nil
}

func LoadSegmentDeltaLogs(ctx context.Context, schema *schemapb.CollectionSchema, collectionID int64, cm storage.ChunkManager, segment DeltaLoadTarget, loadInfo *querypb.SegmentLoadInfo) error {
	deltaLogs := loadInfo.GetDeltalogs()
	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, fmt.Sprintf("LoadDeltalogs-%d", segment.ID()))
	defer sp.End()
	mlog.Debug(ctx, "loading delta...")

	var rowNums int64
	valid := func(binlog *datapb.Binlog, _ int) bool {
		// the segment has applied the delta logs, skip it
		if binlog.GetTimestampTo() > 0 && // this field may be missed in legacy versions
			binlog.GetTimestampTo() < segment.LastDeltaTimestamp() {
			return false
		}
		return true
	}
	for _, deltaLog := range deltaLogs {
		rowNums += lo.SumBy(lo.Filter(deltaLog.GetBinlogs(), valid), func(binlog *datapb.Binlog) int64 {
			return binlog.GetEntriesNum()
		})
	}

	helper, _ := typeutil.CreateSchemaHelper(schema)
	pkField, _ := helper.GetPrimaryKeyField()
	deltaData, err := storage.NewDeltaDataWithPkType(rowNums, pkField.DataType)
	if err != nil {
		return err
	}

	readDeltaRecords := func(reader storage.RecordReader) error {
		defer reader.Close()
		for {
			dl, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			for i := 0; i < dl.Len(); i++ {
				var pk storage.PrimaryKey
				switch pkField.DataType {
				case schemapb.DataType_Int64:
					pk = storage.NewInt64PrimaryKey(dl.Column(0).(*array.Int64).Value(i))
				case schemapb.DataType_VarChar:
					pk = storage.NewVarCharPrimaryKey(dl.Column(0).(*array.String).Value(i))
				}
				ts := typeutil.Timestamp(dl.Column(1).(*array.Int64).Value(i))
				err = deltaData.Append(pk, ts)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	isExternalCollection := typeutil.IsExternalCollection(schema)
	resolver := typeutil.NewStorageColumnResolver(schema)
	if isExternalCollection && !resolver.IsMilvusTable() {
		mlog.Info(ctx, "skip loading delta logs for non-milvus-table external collection")
		return nil
	}
	isMilvusTableRealPK := resolver.IsMilvusTable() && HasExternalPrimaryKey(schema)
	useExplicitDeltalogs := isMilvusTableRealPK && len(deltaLogs) > 0
	readPaths := func(paths []string, opts ...storage.RwOption) error {
		if len(paths) == 0 {
			return nil
		}
		reader, err := storage.NewDeltalogReader(ctx, pkField.DataType, paths, opts...)
		if err != nil {
			return err
		}
		return readDeltaRecords(reader)
	}

	// Manifest-backed delta loading is shared by the parent segment and by
	// compact-to child manifests carried as a load-time delete overlay.
	readManifestDeltas := func(manifestPath string) error {
		if isMilvusTableRealPK {
			// Real-PK milvus-table manifests keep source deltalogs. Target-owned
			// deltalogs are only valid for virtual-PK translation.
			extfs := packed.ExternalSpecContext{
				CollectionID: collectionID,
				Source:       schema.GetExternalSource(),
				Spec:         schema.GetExternalSpec(),
			}
			sourceDeltalogs, err := packed.GetDeltaLogsFromManifestWithExtfs(
				manifestPath,
				createStorageConfig(),
				extfs,
			)
			if err != nil {
				return err
			}
			if err := validateMilvusTableRealPKDeltalogPaths(manifestPath, milvusTableDeltalogPaths(sourceDeltalogs)); err != nil {
				return err
			}
			if len(sourceDeltalogs) > 0 {
				storageV3Paths := make([]string, 0)
				legacyPaths := make([]string, 0)
				for _, deltalog := range sourceDeltalogs {
					for _, binlog := range lo.Filter(deltalog.GetBinlogs(), valid) {
						if packed.IsMilvusTableStorageV3DeltalogPath(binlog.GetLogPath()) {
							storageV3Paths = append(storageV3Paths, binlog.GetLogPath())
						} else {
							legacyPaths = append(legacyPaths, binlog.GetLogPath())
						}
					}
				}
				if len(storageV3Paths) > 0 {
					reader, err := storage.NewDeltalogReader(
						ctx,
						pkField.DataType,
						storageV3Paths,
						storage.WithVersion(storage.StorageV3),
						storage.WithStorageConfig(createStorageConfig()),
						storage.WithExternalReaderContext(extfs),
					)
					if err != nil {
						return err
					}
					if err := readDeltaRecords(reader); err != nil {
						return err
					}
				}
				if len(legacyPaths) > 0 {
					reader, err := storage.NewDeltalogReader(
						ctx,
						pkField.DataType,
						legacyPaths,
						storage.WithVersion(storage.StorageV1),
						storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
							return readExternalFiles(ctx, createStorageConfig(), extfs, paths)
						}),
					)
					if err != nil {
						return err
					}
					if err := readDeltaRecords(reader); err != nil {
						return err
					}
				}
			}
		} else {
			// V3: delta data lives in manifest.
			paths, err := packed.GetDeltaLogPathsFromManifest(manifestPath, createStorageConfig())
			if err != nil {
				return err
			}
			if err := readPaths(paths,
				storage.WithStorageConfig(createStorageConfig()),
				storage.WithVersion(storage.StorageV3),
			); err != nil {
				return err
			}
		}
		return nil
	}

	if manifestPath := loadInfo.GetManifestPath(); manifestPath != "" && !useExplicitDeltalogs {
		if err := readManifestDeltas(manifestPath); err != nil {
			return err
		}
	} else {
		// V1: delta data referenced by Deltalogs entries
		paths := make([]string, 0)
		for _, deltalog := range deltaLogs {
			for _, binlog := range lo.Filter(deltalog.Binlogs, valid) {
				if p := binlog.GetLogPath(); p != "" {
					paths = append(paths, p)
				}
			}
		}
		if err := readPaths(paths,
			storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
				return cm.MultiRead(ctx, paths)
			}),
		); err != nil {
			return err
		}
	}

	// Child manifests are loaded after the parent delete source so all delete
	// records are folded into the same DeltaData before segcore sees the segment.
	for _, manifestPath := range loadInfo.GetChildManifestPaths() {
		if err := readManifestDeltas(manifestPath); err != nil {
			return err
		}
	}

	err = segment.LoadDeltaData(ctx, deltaData)
	if err != nil {
		return err
	}

	mlog.Debug(ctx, "load delta logs done", mlog.Int64("deleteCount", deltaData.DeleteRowCount()))
	return nil
}

func configureUseTakeForOutput(loadInfo *querypb.SegmentLoadInfo, schema *schemapb.CollectionSchema) {
	if loadInfo == nil {
		return
	}
	if typeutil.IsExternalCollection(schema) {
		loadInfo.UseTakeForOutput = paramtable.Get().QueryNodeCfg.ExternalCollectionUseTakeForOutput.GetAsBool()
		return
	}
	loadInfo.UseTakeForOutput = paramtable.Get().QueryNodeCfg.InternalCollectionUseTakeForOutput.GetAsBool()
}

// prepareIndexLoadParams injects QueryNode-local index load parameters into each
// index's IndexParams in place. These params (e.g. DISKANN num_load_thread) are
// derived from local QueryNode resources/config and are never persisted in the
// index metadata, so they must be re-injected on every load path before the load
// info reaches segcore. Both full-load (Load) and Reopen call this; skipping it
// on Reopen was the root cause of issue #51249 (segcore asserts
// "param num_load_thread is empty" while loading a DISKANN index).
func prepareIndexLoadParams(indexInfos []*querypb.FieldIndexInfo) error {
	for _, indexInfo := range indexInfos {
		if indexInfo == nil {
			continue
		}
		indexParams := funcutil.KeyValuePair2Map(indexInfo.GetIndexParams())

		// some build params also exist in indexParams, which are useless during loading process
		if vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexParams["index_type"]) {
			if err := indexparams.SetDiskIndexLoadParams(paramtable.Get(), indexParams, indexInfo.GetNumRows()); err != nil {
				return err
			}
		}

		// set whether enable offset cache for bitmap index
		if indexParams["index_type"] == indexparamcheck.IndexBitmap {
			indexparams.SetBitmapIndexLoadParams(paramtable.Get(), indexParams)
		}

		if err := indexparams.AppendPrepareLoadParams(paramtable.Get(), indexParams); err != nil {
			return err
		}

		indexInfo.IndexParams = funcutil.Map2KeyValuePair(indexParams)
	}
	return nil
}

// PrepareSegmentLoadInfo applies node-local policies to an owned snapshot before
// creating or reopening a native segment. It never consults legacy registries.
func PrepareSegmentLoadInfo(schema *schemapb.CollectionSchema, info *querypb.SegmentLoadInfo) error {
	configureUseTakeForOutput(info, schema)
	return prepareIndexLoadParams(info.GetIndexInfos())
}
