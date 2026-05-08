// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package syncmgr

import (
	"context"
	"path"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type CurrentStatsPayload struct {
	pkFieldID int64
	pkStats   *storage.PrimaryKeyStats
	statsBlob *storage.Blob
	bm25Blobs map[int64]*storage.Blob
}

func PrepareCurrentStats(collectionID int64, schema *schemapb.CollectionSchema, insertData []*storage.InsertData, record storage.Record, batchRows int64) (*CurrentStatsPayload, error) {
	serializer, err := NewStorageSerializerWithCollectionID(collectionID, schema)
	if err != nil {
		return nil, err
	}
	pkStats, statsBlob, err := serializer.SerializeStatslog(insertData, batchRows)
	if err != nil {
		return nil, err
	}
	bm25Collector := storage.NewBm25StatsCollector(schema)
	if err := bm25Collector.Collect(record); err != nil {
		return nil, err
	}
	bm25Blobs, err := bm25Collector.SerializeBlobs()
	if err != nil {
		return nil, err
	}
	return &CurrentStatsPayload{
		pkFieldID: serializer.PKFieldID(),
		pkStats:   pkStats,
		statsBlob: statsBlob,
		bm25Blobs: bm25Blobs,
	}, nil
}

func (p *CurrentStatsPayload) PKFieldID() int64 {
	if p == nil {
		return 0
	}
	return p.pkFieldID
}

func (p *CurrentStatsPayload) StatsBlob() *storage.Blob {
	if p == nil {
		return nil
	}
	return p.statsBlob
}

func (p *CurrentStatsPayload) BM25Blobs() map[int64]*storage.Blob {
	if p == nil {
		return nil
	}
	return p.bm25Blobs
}

type MergedStatsBlobsInput struct {
	SegmentRows     int64
	PreviousBinlogs []*streamingpb.L1SegmentBinLogs
	Current         *CurrentStatsPayload
}

type MergedStatsBlobs struct {
	StatsBlob *storage.Blob
	BM25Blobs map[int64]*storage.Blob
}

func PrepareMergedFieldStatsBlobs(
	ctx context.Context,
	cm storage.ChunkManager,
	collectionID int64,
	schema *schemapb.CollectionSchema,
	input MergedStatsBlobsInput,
) (*MergedStatsBlobs, error) {
	result := &MergedStatsBlobs{}
	serializer, err := NewStorageSerializerWithCollectionID(collectionID, schema)
	if err != nil {
		return nil, err
	}
	stats, err := readPKStats(ctx, cm, statsPaths(previousStatsBinlogs(input.PreviousBinlogs)))
	if err != nil {
		return nil, err
	}
	if input.Current != nil && input.Current.pkStats != nil {
		stats = append(stats, input.Current.pkStats)
	}
	if len(stats) > 0 {
		result.StatsBlob, err = serializer.SerializePKStatsList(stats, input.SegmentRows)
		if err != nil {
			return nil, err
		}
	}

	currentBM25 := map[int64]*storage.Blob(nil)
	if input.Current != nil {
		currentBM25 = input.Current.BM25Blobs()
	}
	result.BM25Blobs, err = mergedBM25FieldBlobsWithCurrent(
		ctx,
		cm,
		previousBM25Binlogs(input.PreviousBinlogs),
		currentBM25,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func PrepareMergedManifestStatsBlobs(
	ctx context.Context,
	cm storage.ChunkManager,
	collectionID int64,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
	manifestPath string,
	segmentRows int64,
	current *CurrentStatsPayload,
) (*MergedStatsBlobs, error) {
	result := &MergedStatsBlobs{}
	serializer, err := NewStorageSerializerWithCollectionID(collectionID, schema)
	if err != nil {
		return nil, err
	}
	manifestStats := make(map[string]packed.ManifestStat)
	if manifestPath != "" {
		manifestStats, err = packed.GetManifestStats(manifestPath, storageConfig)
		if err != nil {
			return nil, err
		}
	}

	pkKey := "bloom_filter." + strconv.FormatInt(serializer.PKFieldID(), 10)
	stats, err := readPKStats(ctx, cm, manifestStats[pkKey].Paths)
	if err != nil {
		return nil, err
	}
	if current != nil && current.pkStats != nil {
		stats = append(stats, current.pkStats)
	}
	if len(stats) > 0 {
		result.StatsBlob, err = serializer.SerializePKStatsList(stats, segmentRows)
		if err != nil {
			return nil, err
		}
	}

	currentBM25 := map[int64]*storage.Blob(nil)
	if current != nil {
		currentBM25 = current.BM25Blobs()
	}
	result.BM25Blobs, err = mergedBM25ManifestBlobsWithCurrent(ctx, cm, manifestStats, currentBM25)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func readPKStats(ctx context.Context, cm storage.ChunkManager, paths []string) ([]*storage.PrimaryKeyStats, error) {
	blobs, err := readBlobs(ctx, cm, nonCompoundPaths(paths))
	if err != nil {
		return nil, err
	}
	return storage.DeserializeStats(blobs)
}

func readBM25Stats(ctx context.Context, cm storage.ChunkManager, paths []string) (*storage.BM25Stats, error) {
	blobs, err := readBlobs(ctx, cm, nonCompoundPaths(paths))
	if err != nil {
		return nil, err
	}
	merged := storage.NewBM25Stats()
	for _, blob := range blobs {
		stats, err := storage.NewBM25StatsWithBytes(blob.Value)
		if err != nil {
			return nil, err
		}
		merged.Merge(stats)
	}
	return merged, nil
}

func statsPaths(groups ...[]*datapb.FieldBinlog) []string {
	var paths []string
	for _, group := range groups {
		for _, binlog := range group {
			for _, log := range binlog.GetBinlogs() {
				if log.GetLogPath() != "" && !isCompoundStatsPath(log.GetLogPath()) {
					paths = append(paths, log.GetLogPath())
				}
			}
		}
	}
	return paths
}

func nonCompoundPaths(paths []string) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if p != "" && !isCompoundStatsPath(p) {
			out = append(out, p)
		}
	}
	return out
}

func isCompoundStatsPath(p string) bool {
	return path.Base(p) == storage.CompoundStatsType.LogIdx()
}

type manifestFieldStats struct {
	files      []string
	memorySize int64
}

func existingManifestStat(
	ctx context.Context,
	cm storage.ChunkManager,
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	key string,
) ([]string, int64) {
	stats, err := packed.GetManifestStats(manifestPath, storageConfig)
	if err != nil {
		return nil, 0
	}
	stat, ok := stats[key]
	if !ok || len(stat.Paths) == 0 {
		return nil, 0
	}
	files := append([]string{}, stat.Paths...)
	return files, manifestStatMemorySize(ctx, cm, stat)
}

func existingManifestBM25Stats(
	ctx context.Context,
	cm storage.ChunkManager,
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) map[int64]*manifestFieldStats {
	fieldMap := make(map[int64]*manifestFieldStats)
	existingStats, err := packed.GetManifestStats(manifestPath, storageConfig)
	if err != nil {
		return fieldMap
	}
	for key, existing := range existingStats {
		prefix, fieldID, ok := packed.ParseStatKey(key)
		if !ok || prefix != "bm25" || len(existing.Paths) == 0 {
			continue
		}
		fieldMap[fieldID] = &manifestFieldStats{
			files:      append([]string{}, existing.Paths...),
			memorySize: manifestStatMemorySize(ctx, cm, existing),
		}
	}
	return fieldMap
}

func ensureManifestFieldStats(fieldMap map[int64]*manifestFieldStats, fieldID int64) *manifestFieldStats {
	stat := fieldMap[fieldID]
	if stat == nil {
		stat = &manifestFieldStats{}
		fieldMap[fieldID] = stat
	}
	return stat
}

func writeManifestStatFile(storageConfig *indexpb.StorageConfig, fullPath string, blob *storage.Blob) (int64, error) {
	if blob == nil {
		return 0, nil
	}
	if err := packed.WriteFile(storageConfig, fullPath, blob.Value); err != nil {
		return 0, err
	}
	return int64(len(blob.Value)), nil
}

func manifestStatMemorySize(ctx context.Context, cm storage.ChunkManager, stat packed.ManifestStat) int64 {
	if memStr, ok := stat.Metadata["memory_size"]; ok {
		if mem, err := strconv.ParseInt(memStr, 10, 64); err == nil {
			return mem
		}
	}
	if cm == nil {
		return 0
	}
	return pathBytes(ctx, cm, stat.Paths)
}

func pathBytes(ctx context.Context, cm storage.ChunkManager, paths []string) int64 {
	var total int64
	for _, p := range paths {
		if size, err := cm.Size(ctx, p); err == nil {
			total += size
			continue
		}
		if value, err := cm.Read(ctx, p); err == nil {
			total += int64(len(value))
		}
	}
	return total
}

func readBlobs(ctx context.Context, cm storage.ChunkManager, paths []string) ([]*storage.Blob, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	values, err := cm.MultiRead(ctx, paths)
	if err != nil {
		return nil, err
	}
	blobs := make([]*storage.Blob, 0, len(values))
	for i, value := range values {
		blobs = append(blobs, &storage.Blob{Key: paths[i], Value: value})
	}
	return blobs, nil
}

func previousStatsBinlogs(previous []*streamingpb.L1SegmentBinLogs) []*datapb.FieldBinlog {
	var out []*datapb.FieldBinlog
	for _, binlog := range previous {
		out = append(out, binlog.GetStatsBinlog()...)
	}
	return out
}

func previousBM25Binlogs(previous []*streamingpb.L1SegmentBinLogs) []*datapb.FieldBinlog {
	var out []*datapb.FieldBinlog
	for _, binlog := range previous {
		out = append(out, binlog.GetBm25Binlog()...)
	}
	return out
}

func mergedBM25FieldBlobsWithCurrent(
	ctx context.Context,
	cm storage.ChunkManager,
	previous []*datapb.FieldBinlog,
	current map[int64]*storage.Blob,
) (map[int64]*storage.Blob, error) {
	byField := make(map[int64][]string)
	for _, binlog := range previous {
		for _, log := range binlog.GetBinlogs() {
			if !isCompoundStatsPath(log.GetLogPath()) && log.GetLogPath() != "" {
				byField[binlog.GetFieldID()] = append(byField[binlog.GetFieldID()], log.GetLogPath())
			}
		}
	}
	return mergedBM25Blobs(ctx, cm, byField, current)
}

func mergedBM25ManifestBlobsWithCurrent(
	ctx context.Context,
	cm storage.ChunkManager,
	manifestStats map[string]packed.ManifestStat,
	current map[int64]*storage.Blob,
) (map[int64]*storage.Blob, error) {
	byField := make(map[int64][]string)
	for key, stat := range manifestStats {
		prefix, fieldID, ok := packed.ParseStatKey(key)
		if !ok || prefix != "bm25" || len(stat.Paths) == 0 {
			continue
		}
		byField[fieldID] = stat.Paths
	}
	return mergedBM25Blobs(ctx, cm, byField, current)
}

func mergedBM25Blobs(
	ctx context.Context,
	cm storage.ChunkManager,
	previous map[int64][]string,
	current map[int64]*storage.Blob,
) (map[int64]*storage.Blob, error) {
	mergedStats := make(map[int64]*storage.BM25Stats)
	for fieldID, paths := range previous {
		stats, err := readBM25Stats(ctx, cm, paths)
		if err != nil {
			return nil, err
		}
		if stats != nil && stats.NumRow() > 0 {
			mergedStats[fieldID] = stats
		}
	}
	for fieldID, blob := range current {
		if blob == nil || len(blob.Value) == 0 {
			continue
		}
		stats, err := storage.NewBM25StatsWithBytes(blob.Value)
		if err != nil {
			return nil, err
		}
		if stats.NumRow() == 0 {
			continue
		}
		if mergedStats[fieldID] == nil {
			mergedStats[fieldID] = storage.NewBM25Stats()
		}
		mergedStats[fieldID].Merge(stats)
	}

	mergedBlobs := make(map[int64]*storage.Blob)
	for fieldID, stats := range mergedStats {
		if stats == nil || stats.NumRow() == 0 {
			continue
		}
		bytes, err := stats.Serialize()
		if err != nil {
			return nil, err
		}
		mergedBlobs[fieldID] = &storage.Blob{
			Value:      bytes,
			MemorySize: int64(len(bytes)),
			RowNum:     stats.NumRow(),
		}
	}
	return mergedBlobs, nil
}
