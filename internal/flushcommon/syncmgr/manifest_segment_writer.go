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
	"fmt"
	"path"
	"strconv"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// ManifestSegmentWriter tracks the current manifest path while writing storage-v3 data.
type ManifestSegmentWriter struct {
	*PackedSegmentWriter

	ManifestPath string
}

func (w *ManifestSegmentWriter) WriteManifestInsert(ctx context.Context, record storage.Record) (map[int64]*datapb.FieldBinlog, string, error) {
	rootPath := w.ChunkManager.RootPath()
	if w.StorageConfig != nil {
		rootPath = w.StorageConfig.GetRootPath()
	}
	basePath, version, err := ManifestBaseAndVersion(rootPath, w.CollectionID, w.PartitionID, w.SegmentID, w.ManifestPath)
	if err != nil {
		return nil, "", err
	}
	multiPartUploadSize := w.MultiPartUploadSize
	if multiPartUploadSize == 0 {
		multiPartUploadSize = packed.DefaultMultiPartUploadSize
	}
	var writer manifestRecordWriter
	textColumnConfigs := buildTextColumnConfigs(w.Schema, path.Dir(basePath))
	if len(textColumnConfigs) > 0 {
		writer, err = storage.NewPackedTextManifestWriter("", basePath, version, w.Schema,
			w.BufferSize, multiPartUploadSize, w.ColumnGroups, w.StorageConfig, textColumnConfigs)
	} else {
		writer, err = storage.NewPackedRecordManifestWriter(basePath, version, w.Schema,
			w.BufferSize, multiPartUploadSize, w.ColumnGroups, w.StorageConfig, w.PluginContext)
	}
	if err != nil {
		return nil, "", err
	}
	if err := writer.Write(record); err != nil {
		_ = writer.Close()
		return nil, "", err
	}
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	logs := make(map[int64]*datapb.FieldBinlog)
	for _, columnGroup := range w.ColumnGroups {
		columnGroupID := columnGroup.GroupID
		logs[columnGroupID] = &datapb.FieldBinlog{
			FieldID:     columnGroupID,
			ChildFields: columnGroup.Fields,
			Binlogs: []*datapb.Binlog{{
				LogSize:         int64(writer.GetColumnGroupWrittenCompressed(columnGroup.GroupID)),
				MemorySize:      int64(writer.GetColumnGroupWrittenUncompressed(columnGroup.GroupID)),
				LogPath:         writer.GetWrittenPaths(columnGroupID),
				EntriesNum:      writer.GetWrittenRowNum(),
				TimestampFrom:   w.TsFrom,
				TimestampTo:     w.TsTo,
				FieldNullCounts: fieldNullCounts(record, columnGroup),
			}},
		}
	}
	manifestPath := writer.GetWrittenManifest()
	w.ManifestPath = manifestPath
	return logs, manifestPath, nil
}

type ManifestPKStatsInput struct {
	FieldID    int64
	BatchBlob  *storage.Blob
	MergedBlob *storage.Blob
}

func (w *ManifestSegmentWriter) WritePKStatsBlobs(ctx context.Context, input ManifestPKStatsInput) (string, int64, error) {
	if input.BatchBlob == nil && input.MergedBlob == nil {
		return w.ManifestPath, 0, nil
	}
	basePath, _, err := packed.UnmarshalManifestPath(w.ManifestPath)
	if err != nil {
		return "", 0, err
	}

	statKey := fmt.Sprintf("bloom_filter.%d", input.FieldID)
	files, memorySize := existingManifestStat(ctx, w.ChunkManager, w.ManifestPath, w.StorageConfig, statKey)
	var size int64

	if input.BatchBlob != nil {
		id, err := w.Allocator.AllocOne()
		if err != nil {
			return "", 0, err
		}
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/bloom_filter.%d/%d", input.FieldID, id))
		written, err := writeManifestStatFile(w.StorageConfig, fullPath, input.BatchBlob)
		if err != nil {
			return "", 0, err
		}
		files = append(files, fullPath)
		memorySize += written
		size += written
	}
	if input.MergedBlob != nil {
		files = nonCompoundPaths(files)
		memorySize = pathBytes(ctx, w.ChunkManager, files)
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/bloom_filter.%d/%d", input.FieldID, int64(storage.CompoundStatsType)))
		written, err := writeManifestStatFile(w.StorageConfig, fullPath, input.MergedBlob)
		if err != nil {
			return "", 0, err
		}
		files = append(files, fullPath)
		memorySize += written
		size += written
	}

	manifestPath, err := packed.AddStatsToManifest(w.ManifestPath, w.StorageConfig, []packed.StatEntry{{
		Key:      statKey,
		Files:    files,
		Metadata: map[string]string{"memory_size": strconv.FormatInt(memorySize, 10)},
	}})
	if err != nil {
		return "", 0, fmt.Errorf("failed to add stats to manifest: %w", err)
	}
	w.ManifestPath = manifestPath
	return manifestPath, size, nil
}

type ManifestBM25StatsInput struct {
	BatchBlobs  map[int64]*storage.Blob
	MergedBlobs map[int64]*storage.Blob
}

func (w *ManifestSegmentWriter) WriteBM25StatsBlobs(ctx context.Context, input ManifestBM25StatsInput) (string, int64, error) {
	if len(input.BatchBlobs) == 0 && len(input.MergedBlobs) == 0 {
		return w.ManifestPath, 0, nil
	}
	basePath, _, err := packed.UnmarshalManifestPath(w.ManifestPath)
	if err != nil {
		return "", 0, err
	}

	fieldMap := existingManifestBM25Stats(ctx, w.ChunkManager, w.ManifestPath, w.StorageConfig)
	var size int64
	for fieldID, blob := range input.BatchBlobs {
		id, err := w.Allocator.AllocOne()
		if err != nil {
			return "", 0, err
		}
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/bm25.%d/%d", fieldID, id))
		written, err := writeManifestStatFile(w.StorageConfig, fullPath, blob)
		if err != nil {
			return "", 0, err
		}
		stat := ensureManifestFieldStats(fieldMap, fieldID)
		stat.files = append(stat.files, fullPath)
		stat.memorySize += written
		size += written
	}
	for fieldID, blob := range input.MergedBlobs {
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/bm25.%d/%d", fieldID, int64(storage.CompoundStatsType)))
		written, err := writeManifestStatFile(w.StorageConfig, fullPath, blob)
		if err != nil {
			return "", 0, err
		}
		stat := ensureManifestFieldStats(fieldMap, fieldID)
		stat.files = nonCompoundPaths(stat.files)
		stat.memorySize = pathBytes(ctx, w.ChunkManager, stat.files)
		stat.files = append(stat.files, fullPath)
		stat.memorySize += written
		size += written
	}

	statEntries := make([]packed.StatEntry, 0, len(fieldMap))
	for fieldID, stat := range fieldMap {
		statEntries = append(statEntries, packed.StatEntry{
			Key:      fmt.Sprintf("bm25.%d", fieldID),
			Files:    stat.files,
			Metadata: map[string]string{"memory_size": strconv.FormatInt(stat.memorySize, 10)},
		})
	}
	if len(statEntries) == 0 {
		return w.ManifestPath, size, nil
	}
	manifestPath, err := packed.AddStatsToManifest(w.ManifestPath, w.StorageConfig, statEntries)
	if err != nil {
		return "", 0, fmt.Errorf("failed to add BM25 stats to manifest: %w", err)
	}
	w.ManifestPath = manifestPath
	return manifestPath, size, nil
}
