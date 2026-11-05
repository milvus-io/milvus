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

package inspector

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestLocateRawDataV2EnumeratesEveryAuthoritativeBinlog(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{
			ID: 31, CollectionID: 11, PartitionID: 21, StorageVersion: 2,
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "raw/a"}, {LogID: 302}}},
				{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: "raw/c"}}},
			},
		},
	}

	objects, err := LocateRawDataV2("files", segments)
	require.NoError(t, err)
	require.Equal(t, []RawDataObject{
		{CollectionID: 11, PartitionID: 21, SegmentID: 31, FieldID: 101, Path: "raw/a", StorageVersion: 2},
		{CollectionID: 11, PartitionID: 21, SegmentID: 31, FieldID: 101, Path: "files/insert_log/11/21/31/101/302", StorageVersion: 2},
		{CollectionID: 11, PartitionID: 21, SegmentID: 31, FieldID: 102, Path: "raw/c", StorageVersion: 2},
	}, objects)
}

func TestLocateRawDataV2RejectsWrongVersionAndEmptyPaths(t *testing.T) {
	_, err := LocateRawDataV2("files", []*datapb.SegmentInfo{{ID: 31, StorageVersion: 3}})
	require.ErrorContains(t, err, "storage version 3")

	_, err = LocateRawDataV2("files", []*datapb.SegmentInfo{{
		ID: 31, StorageVersion: 2,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: ""}}}},
	}})
	require.ErrorContains(t, err, "neither a raw-data object path nor a valid log ID")

	_, err = LocateRawDataV2("files", []*datapb.SegmentInfo{
		{ID: 31, CollectionID: 11, StorageVersion: 2, Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "raw/a"}}}}},
		{ID: 32, CollectionID: 12, StorageVersion: 2, Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "raw/b"}}}}},
	})
	require.ErrorContains(t, err, "belongs to collection 12")
}

func TestLocateRawDataV2RejectsIncompleteObjectSets(t *testing.T) {
	for _, test := range []struct {
		name     string
		segments []*datapb.SegmentInfo
		want     string
	}{
		{name: "no segments", want: "no sealed segments"},
		{name: "no fields", segments: []*datapb.SegmentInfo{{ID: 31, StorageVersion: 2}}, want: "no raw-data FieldBinlog"},
		{name: "no objects", segments: []*datapb.SegmentInfo{{
			ID: 31, StorageVersion: 2,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 101}},
		}}, want: "no raw-data Binlog"},
		{name: "duplicate objects", segments: []*datapb.SegmentInfo{{
			ID: 31, StorageVersion: 2,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "raw/a"}, {LogPath: "raw/a"}}}},
		}}, want: "reported more than once"},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects, err := LocateRawDataV2("files", test.segments)
			require.ErrorContains(t, err, test.want)
			require.Nil(t, objects)
		})
	}
}
