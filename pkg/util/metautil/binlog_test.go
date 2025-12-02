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

package metautil

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestParseInsertLogPath(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name             string
		args             args
		wantCollectionID typeutil.UniqueID
		wantPartitionID  typeutil.UniqueID
		wantSegmentID    typeutil.UniqueID
		wantFieldID      typeutil.UniqueID
		wantLogID        typeutil.UniqueID
		wantOk           bool
	}{
		{
			"test parse insert log path",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/446266956600703326/447985737531772787/102/447985737523710526"},
			446266956600703270,
			446266956600703326,
			447985737531772787,
			102,
			447985737523710526,
			true,
		},

		{
			"test parse insert log path negative1",
			args{path: "foobar"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative2",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/446266956600703326/447985737531772787/102/foo"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative3",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/446266956600703326/447985737531772787/foo/447985737523710526"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative4",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/446266956600703326/foo/102/447985737523710526"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative5",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/foo/447985737531772787/102/447985737523710526"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative6",
			args{path: "8a8c3ac2298b12f/insert_log/foo/446266956600703326/447985737531772787/102/447985737523710526"},
			0,
			0,
			0,
			0,
			0,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCollectionID, gotPartitionID, gotSegmentID, gotFieldID, gotLogID, gotOk := ParseInsertLogPath(tt.args.path)
			if !reflect.DeepEqual(gotCollectionID, tt.wantCollectionID) {
				t.Errorf("ParseInsertLogPath() gotCollectionID = %v, want %v", gotCollectionID, tt.wantCollectionID)
			}
			if !reflect.DeepEqual(gotPartitionID, tt.wantPartitionID) {
				t.Errorf("ParseInsertLogPath() gotPartitionID = %v, want %v", gotPartitionID, tt.wantPartitionID)
			}
			if !reflect.DeepEqual(gotSegmentID, tt.wantSegmentID) {
				t.Errorf("ParseInsertLogPath() gotSegmentID = %v, want %v", gotSegmentID, tt.wantSegmentID)
			}
			if !reflect.DeepEqual(gotFieldID, tt.wantFieldID) {
				t.Errorf("ParseInsertLogPath() gotFieldID = %v, want %v", gotFieldID, tt.wantFieldID)
			}
			if !reflect.DeepEqual(gotLogID, tt.wantLogID) {
				t.Errorf("ParseInsertLogPath() gotLogID = %v, want %v", gotLogID, tt.wantLogID)
			}
			if gotOk != tt.wantOk {
				t.Errorf("ParseInsertLogPath() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestBuildLOBLogPath(t *testing.T) {
	tests := []struct {
		name         string
		rootPath     string
		collectionID typeutil.UniqueID
		partitionID  typeutil.UniqueID
		fieldID      typeutil.UniqueID
		lobID        typeutil.UniqueID
		want         string
	}{
		{
			name:         "normal path",
			rootPath:     "files",
			collectionID: 100,
			partitionID:  200,
			fieldID:      101,
			lobID:        12345,
			want:         "files/insert_log/100/200/lobs/101/12345",
		},
		{
			name:         "empty root path",
			rootPath:     "",
			collectionID: 100,
			partitionID:  200,
			fieldID:      101,
			lobID:        12345,
			want:         "insert_log/100/200/lobs/101/12345",
		},
		{
			name:         "large IDs",
			rootPath:     "data",
			collectionID: 446266956600703270,
			partitionID:  446266956600703326,
			fieldID:      102,
			lobID:        453718835200262143,
			want:         "data/insert_log/446266956600703270/446266956600703326/lobs/102/453718835200262143",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildLOBLogPath(tt.rootPath, tt.collectionID, tt.partitionID, tt.fieldID, tt.lobID)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseLOBFilePath(t *testing.T) {
	// New path format: {root}/insert_log/{coll}/{part}/lobs/{field}/{lob_id}
	tests := []struct {
		name        string
		filePath    string
		wantFieldID int64
		wantLobID   uint64
		wantOk      bool
	}{
		{
			name:        "valid path",
			filePath:    "files/insert_log/100/200/lobs/101/12345",
			wantFieldID: 101,
			wantLobID:   12345,
			wantOk:      true,
		},
		{
			name:        "valid path with large IDs",
			filePath:    "data/insert_log/446266956600703270/446266956600703326/lobs/102/453718835200262143",
			wantFieldID: 102,
			wantLobID:   453718835200262143,
			wantOk:      true,
		},
		{
			name:        "path without root",
			filePath:    "insert_log/100/200/lobs/101/12345",
			wantFieldID: 101,
			wantLobID:   12345,
			wantOk:      true,
		},
		{
			name:        "no lobs in path",
			filePath:    "files/insert_log/100/200/101/12345",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
		{
			name:        "invalid field ID",
			filePath:    "files/insert_log/100/200/lobs/abc/12345",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
		{
			name:        "invalid lob file ID",
			filePath:    "files/insert_log/100/200/lobs/101/abc",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
		{
			name:        "too short path",
			filePath:    "lobs",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
		{
			name:        "empty path",
			filePath:    "",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
		{
			name:        "lobs at start",
			filePath:    "lobs/12345",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
		{
			name:        "lobs at end without file",
			filePath:    "101/lobs",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
		{
			name:        "no insert_log in path",
			filePath:    "files/100/200/lobs/101/12345",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
		{
			name:        "blob in path should not match",
			filePath:    "files/blob/insert_log/100/200/lobs/101/12345",
			wantFieldID: 101,
			wantLobID:   12345,
			wantOk:      true,
		},
		{
			name:        "lobs in wrong position",
			filePath:    "files/insert_log/100/lobs/200/101/12345",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
		{
			name:        "missing partition before lobs",
			filePath:    "files/insert_log/100/lobs/101/12345",
			wantFieldID: 0,
			wantLobID:   0,
			wantOk:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFieldID, gotLobID, gotOk := ParseLOBFilePath(tt.filePath)
			assert.Equal(t, tt.wantOk, gotOk)
			if tt.wantOk {
				assert.Equal(t, tt.wantFieldID, gotFieldID)
				assert.Equal(t, tt.wantLobID, gotLobID)
			}
		})
	}
}

func TestExtractLOBFileID(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		want     uint64
		wantErr  bool
	}{
		{
			name:     "valid path",
			filePath: "files/insert_log/100/200/lobs/101/12345",
			want:     12345,
			wantErr:  false,
		},
		{
			name:     "large ID",
			filePath: "data/insert_log/100/200/lobs/101/453718835200262143",
			want:     453718835200262143,
			wantErr:  false,
		},
		{
			name:     "simple path",
			filePath: "12345",
			want:     12345,
			wantErr:  false,
		},
		{
			name:     "invalid - non-numeric",
			filePath: "files/insert_log/100/200/lobs/101/abc",
			want:     0,
			wantErr:  true,
		},
		{
			name:     "invalid - with extension",
			filePath: "files/insert_log/100/200/lobs/101/12345.parquet",
			want:     0,
			wantErr:  true,
		},
		{
			name:     "empty path",
			filePath: "",
			want:     0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractLOBFileID(tt.filePath)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestBuildLOBLogPath_RoundTrip(t *testing.T) {
	// Test that BuildLOBLogPath and ParseLOBFilePath work together
	// New path format: {root}/insert_log/{coll}/{part}/lobs/{field}/{lob_id}
	rootPath := "files"
	collectionID := typeutil.UniqueID(100)
	partitionID := typeutil.UniqueID(200)
	fieldID := typeutil.UniqueID(101)
	lobID := typeutil.UniqueID(12345)

	path := BuildLOBLogPath(rootPath, collectionID, partitionID, fieldID, lobID)

	gotFieldID, gotLobID, ok := ParseLOBFilePath(path)
	require.True(t, ok)
	assert.Equal(t, int64(fieldID), gotFieldID)
	assert.Equal(t, uint64(lobID), gotLobID)

	gotLobID2, err := ExtractLOBFileID(path)
	require.NoError(t, err)
	assert.Equal(t, uint64(lobID), gotLobID2)
}
