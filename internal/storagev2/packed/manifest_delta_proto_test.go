// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package packed

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColumnGroupEntriesProtoRoundTrip pins the wire fidelity the DataCoord-side
// manifest commit depends on: every field a loon column-group add needs — column
// names, format, and per-file path / inclusive-start / exclusive-end / arbitrary
// properties — must survive datanode->DataCoord marshalling unchanged, or the
// rebuilt transaction would register different files than the datanode wrote.
func TestColumnGroupEntriesProtoRoundTrip(t *testing.T) {
	entries := []ColumnGroupEntry{
		{
			Columns: []string{"pk", "sparse"},
			Format:  "parquet",
			Files: []ColumnGroupFileEntry{
				{
					Path:       "/tmp/milvus/insert_log/1/2/100/cg0/9001",
					StartIndex: 0,
					EndIndex:   3,
					Properties: map[string]string{"encoding": "plain", "codec": "zstd"},
				},
				{
					Path:       "/tmp/milvus/insert_log/1/2/100/cg0/9002",
					StartIndex: 3,
					EndIndex:   7,
					// nil properties must round-trip as absent, not panic.
				},
			},
		},
		{
			// A column group with no files and no properties is still a legal
			// descriptor and must not collapse to nil.
			Columns: []string{"vec"},
			Format:  "parquet",
		},
	}

	got := ColumnGroupEntriesFromProto(ColumnGroupEntriesToProto(entries))
	require.Len(t, got, len(entries))
	assert.Equal(t, entries[0].Columns, got[0].Columns)
	assert.Equal(t, entries[0].Format, got[0].Format)
	require.Len(t, got[0].Files, 2)
	assert.Equal(t, entries[0].Files[0].Path, got[0].Files[0].Path)
	assert.EqualValues(t, 0, got[0].Files[0].StartIndex)
	assert.EqualValues(t, 3, got[0].Files[0].EndIndex)
	assert.Equal(t, "plain", got[0].Files[0].Properties["encoding"])
	assert.Equal(t, "zstd", got[0].Files[0].Properties["codec"])
	assert.EqualValues(t, 3, got[0].Files[1].StartIndex)
	assert.EqualValues(t, 7, got[0].Files[1].EndIndex)
	assert.Empty(t, got[0].Files[1].Properties)
	assert.Equal(t, []string{"vec"}, got[1].Columns)
	assert.Empty(t, got[1].Files)
}

// TestColumnGroupEntriesProtoEmpty documents that empty/nil inputs map to nil on
// both directions, so an isEmpty() manifest update stays empty across the wire.
func TestColumnGroupEntriesProtoEmpty(t *testing.T) {
	assert.Nil(t, ColumnGroupEntriesToProto(nil))
	assert.Nil(t, ColumnGroupEntriesToProto([]ColumnGroupEntry{}))
	assert.Nil(t, ColumnGroupEntriesFromProto(nil))
}

// TestStatEntriesProtoRoundTrip covers the BM25/bloom stat entries that ride in
// the same manifest delta as the column groups.
func TestStatEntriesProtoRoundTrip(t *testing.T) {
	entries := []StatEntry{
		{
			Key:      "bm25.102",
			Files:    []string{"stats/bm25/9003", "stats/bm25/9004"},
			Metadata: map[string]string{"num_rows": "3"},
		},
		{
			Key:   "bloom_filter.100",
			Files: []string{"stats/bf/9005"},
		},
	}

	got := StatEntriesFromProto(StatEntriesToProto(entries))
	require.Len(t, got, len(entries))
	assert.Equal(t, entries[0].Key, got[0].Key)
	assert.Equal(t, entries[0].Files, got[0].Files)
	assert.Equal(t, "3", got[0].Metadata["num_rows"])
	assert.Equal(t, entries[1].Key, got[1].Key)
	assert.Equal(t, entries[1].Files, got[1].Files)
	assert.Empty(t, got[1].Metadata)

	assert.Nil(t, StatEntriesToProto(nil))
	assert.Nil(t, StatEntriesFromProto(nil))
}
