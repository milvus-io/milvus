// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// TestPackedManifestRecordWriter_CloseWithoutWrite verifies the no-data
// short-circuit: Close on a freshly-constructed PackedManifestRecordWriter
// must not call into the FFI commit path and must not produce a manifest.
func TestPackedManifestRecordWriter_CloseWithoutWrite(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}

	dir := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}

	w, err := newPackedManifestRecordWriter(1, 2, 3, schema,
		ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
		allocator.NewLocalAllocator(1, 1<<20),
		1024, 0, 0, nil, cfg, nil)
	require.NoError(t, err)

	// No Write before Close. The internal `writer` field stays nil so
	// Close must return immediately without invoking the FFI commit.
	require.NoError(t, w.Close())

	_, statsLog, _, manifestPath, _ := w.GetLogs()
	assert.Empty(t, manifestPath, "no Write means no manifest should be produced")
	assert.Nil(t, statsLog, "no Write means no statsLog should be produced")
}

// TestPackedTextManifestRecordWriter_CloseWithoutWrite is the parallel
// short-circuit test for the text writer: Close with no Writes must
// still succeed without touching the FFI segment writer.
func TestPackedTextManifestRecordWriter_CloseWithoutWrite(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_Text, Name: "doc"},
	}}

	dir := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}

	w, err := NewPackedTextManifestRecordWriter(1, 2, 3, schema,
		ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
		allocator.NewLocalAllocator(1, 1<<20),
		1024, 0, 0, nil, cfg, nil)
	require.NoError(t, err)

	// No Write before Close. The text writer's nil-handling path must
	// fall through to writeStats() and exit cleanly.
	require.NoError(t, w.Close())

	_, _, _, manifestPath, _ := w.GetLogs()
	assert.Empty(t, manifestPath, "no Write means no manifest should be produced")
}
