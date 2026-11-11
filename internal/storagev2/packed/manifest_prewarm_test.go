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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestManifestUpdatesNewObjects(t *testing.T) {
	updates := &ManifestUpdates{
		ColumnGroups: []ColumnGroupEntry{{Files: []ColumnGroupFileEntry{{Path: "files//segment/data", Properties: map[string]string{"file_size": "11"}}, {Path: "unknown"}}}},
		DeltaLogs:    []DeltaLogEntry{{Path: "files//segment/delta", FileSize: 12}},
		Stats:        []StatEntry{{Files: []string{"files//segment/stats"}, FileSizes: map[string]uint64{"files//segment/stats": 13}}},
		Indexes:      []ManifestIndexInfo{{Path: "123/1", IndexFileKeys: []string{"index"}, SerializedSize: 14}},
		DropIndexes:  []DropIndexEntry{{IndexID: 1}},
	}
	objects, missing, err := updates.NewObjects("files//segment")
	require.NoError(t, err)
	require.Equal(t, []PrewarmObject{{Path: "files//segment/data", Size: 11}, {Path: "files//segment/delta", Size: 12}, {Path: "files//segment/stats", Size: 13}, {Path: "files//segment/_index/123/1/index", Size: 14}}, objects)
	require.Equal(t, []string{"unknown"}, missing)
	updates.Stats[0].FileSizes["files//segment/stats"] = 100
	require.EqualValues(t, 13, objects[2].Size)
	objects, missing, err = (&ManifestUpdates{DropIndexes: []DropIndexEntry{{IndexID: 1}}}).NewObjects("files/segment")
	require.NoError(t, err)
	require.Empty(t, objects)
	require.Empty(t, missing)
	var empty *ManifestUpdates
	objects, _, err = empty.NewObjects("files/segment")
	require.NoError(t, err)
	require.Empty(t, objects)
}

func TestManifestPrewarmDoesNotGuessMultiFileIndexSizes(t *testing.T) {
	updates := &ManifestUpdates{Indexes: []ManifestIndexInfo{{Path: "1/1", IndexFileKeys: []string{"a", "b"}, SerializedSize: 100}}}
	objects, missing, err := updates.NewObjects("segment")
	require.NoError(t, err)
	require.Empty(t, objects)
	require.Len(t, missing, 2)
	updates.Indexes[0].FileSizes = map[string]uint64{"a": 37, "b": 63}
	objects, missing, err = updates.NewObjects("segment")
	require.NoError(t, err)
	require.Empty(t, missing)
	require.EqualValues(t, 37, objects[0].Size)
	require.EqualValues(t, 63, objects[1].Size)
}

func TestManifestPrewarmUsesDiskSize(t *testing.T) {
	stat := FieldBinlogStatEntry("bloom_filter", 100, &datapb.FieldBinlog{Binlogs: []*datapb.Binlog{{LogPath: "stats", LogSize: 17, MemorySize: 999}}})
	objects, missing, err := (&ManifestUpdates{Stats: []StatEntry{stat}}).NewObjects("segment")
	require.NoError(t, err)
	require.Empty(t, missing)
	require.Equal(t, []PrewarmObject{{Path: "stats", Size: 17}}, objects)
}
