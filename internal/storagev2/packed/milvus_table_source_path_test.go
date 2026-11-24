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

package packed

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus/internal/snapshotio"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The source snapshot lives in bucket "snapshots" behind endpoint storage:9000.
var milvusTableTestExtfs = ExternalSpecContext{
	CollectionID: 42,
	Source:       "s3://snapshots/metadata/v1.json",
	Spec: `{"format":"milvus-table","extfs":{"cloud_provider":"minio","endpoint_url":"http://storage:9000",` +
		`"region":"us-east-1","access_key_id":"ak","access_key_value":"sk"}}`,
}

const milvusTableTestExternalPrefix = "s3://storage:9000/snapshots/"

func TestCheckMilvusTableExternalPath(t *testing.T) {
	for _, tc := range []struct {
		path     string
		external bool
	}{
		{path: "s3://storage:9000/snapshots/files/insert_log/1/2/3", external: true},
		{path: "minio://storage:9000/snapshots/files/insert_log/1/2/3", external: true},
		{path: "s3:///snapshots/files/insert_log/1/2/3", external: true},
		{path: "/var/lib/milvus/data/insert_log/1/2/3"},
		{path: "files/insert_log/1/2/3"},
		{path: "../files/insert_log/1/2/3"},
		{path: ""},
		// milvus-storage cannot parse it as a URI and treats it as a path.
		{path: "s3://storage:9000/snapshots/files/insert_log/1 2"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			err := checkMilvusTableExternalPath(tc.path, tc.path)
			if tc.external {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, "external source storage")
		})
	}
}

func milvusTableTestSnapshot(t *testing.T, manifestPath string, basePath string) []byte {
	t.Helper()
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: snapshotio.SnapshotFormatVersion,
		ManifestList:  []string{manifestPath},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{{
			SegmentId: 10,
			Manifest:  MarshalManifestPath(basePath, 2),
		}},
	}
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)
	return data
}

func milvusTableTestSegment(t *testing.T, segment *datapb.SegmentDescription) []byte {
	t.Helper()
	data, err := snapshotio.MarshalSegmentManifest(segment)
	require.NoError(t, err)
	return data
}

// exploreMilvusTableTestSource explores a snapshot served by a fake external
// store and returns the paths the explore read.
func exploreMilvusTableTestSource(t *testing.T, objects map[string][]byte) ([]FileInfo, []string, error) {
	t.Helper()
	config := manifestTestStorageConfig(t)
	dir := config.GetRootPath()
	var reads []string
	patch := mockey.Mock(readFileWithExternalSpec).
		To(func(_ *indexpb.StorageConfig, path string, _ ExternalSpecContext, _ externalPathForm) ([]byte, error) {
			reads = append(reads, path)
			data, ok := objects[path]
			if !ok {
				return nil, merr.WrapErrIoKeyNotFound(path)
			}
			return data, nil
		}).Build()
	defer patch.UnPatch()
	infos, _, err := ExploreFilesReturnManifestPath(nil, "milvus-table", dir, milvusTableTestExtfs.Source, config, milvusTableTestExtfs)
	return infos, reads, err
}

func TestExploreMilvusTableResolvesSnapshotPathsInExternalSource(t *testing.T) {
	segment := milvusTableTestSegment(t, &datapb.SegmentDescription{
		SegmentId: 10, SegmentLevel: datapb.SegmentLevel_L1, NumOfRows: 100,
	})
	infos, reads, err := exploreMilvusTableTestSource(t, map[string][]byte{
		milvusTableTestExtfs.Source:                        milvusTableTestSnapshot(t, "metadata/10.avro", "files/insert_log/1/2/10"),
		milvusTableTestExternalPrefix + "metadata/10.avro": segment,
	})
	require.NoError(t, err)
	require.Equal(t, []string{milvusTableTestExtfs.Source, milvusTableTestExternalPrefix + "metadata/10.avro"}, reads)
	require.Len(t, infos, 1)
	require.Equal(t, MarshalManifestPath(milvusTableTestExternalPrefix+"files/insert_log/1/2/10", 2), infos[0].FilePath)
}

func TestExploreMilvusTableRejectsSnapshotPathsOutsideExternalSource(t *testing.T) {
	const localSegment = "/var/lib/milvus/data/insert_log/1/2/10"
	l1 := &datapb.SegmentDescription{SegmentId: 10, SegmentLevel: datapb.SegmentLevel_L1, NumOfRows: 100}
	l0 := &datapb.SegmentDescription{
		SegmentId: 20, SegmentLevel: datapb.SegmentLevel_L0,
		Deltalogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{
			LogPath: "/var/lib/milvus/data/delta_log/1/2/20/7", EntriesNum: 1,
		}}}},
	}
	for _, tc := range []struct {
		name     string
		manifest string
		base     string
		segment  *datapb.SegmentDescription
		badPath  string
	}{
		{name: "absolute manifest base path", manifest: "metadata/10.avro", base: localSegment, segment: l1, badPath: localSegment},
		{name: "absolute segment manifest", manifest: "/var/lib/milvus/data/snapshots/1/manifests/10.avro", base: "files/insert_log/1/2/10", segment: l1, badPath: "/var/lib/milvus/data/snapshots/1/manifests/10.avro"},
		{name: "absolute deltalog", manifest: "metadata/10.avro", base: "files/insert_log/1/2/10", segment: l0, badPath: "/var/lib/milvus/data/delta_log/1/2/20/7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			infos, reads, err := exploreMilvusTableTestSource(t, map[string][]byte{
				milvusTableTestExtfs.Source:                        milvusTableTestSnapshot(t, tc.manifest, tc.base),
				milvusTableTestExternalPrefix + "metadata/10.avro": milvusTableTestSegment(t, tc.segment),
			})
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, "external source storage: "+tc.badPath)
			require.Nil(t, infos)
			for _, read := range reads {
				require.False(t, strings.HasPrefix(read, "/"), "read %s outside the external source", read)
			}
		})
	}
}

func TestResolveMilvusTableSourceColumnGroups(t *testing.T) {
	config := manifestTestStorageConfig(t)
	props, err := MakePropertiesFromStorageConfig(config, nil)
	require.NoError(t, err)
	defer FreeProperties(props)
	require.NoError(t, injectExternalSpecProperties(props, milvusTableTestExtfs.CollectionID, milvusTableTestExtfs.Source, milvusTableTestExtfs.Spec))

	resolve := func(path string) (string, error) {
		groups, err := createColumnGroups([]string{"100"}, "parquet", []Fragment{{FilePath: path, EndRow: 1}})
		require.NoError(t, err)
		source := &ColumnGroups{cColumnGroups: groups}
		defer source.Destroy()
		resolvedGroups, err := resolveMilvusTableSourceColumnGroups(groups, props, milvusTableTestExtfs)
		if err != nil {
			return "", err
		}
		resolved := &ColumnGroups{cColumnGroups: resolvedGroups}
		defer resolved.Destroy()
		entries, err := columnGroupEntriesFromC(resolvedGroups)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Len(t, entries[0].Files, 1)
		return entries[0].Files[0].Path, nil
	}

	// Relative paths of a source manifest are keys of the external source.
	got, err := resolve("files/insert_log/1/2/10/_data/1.parquet")
	require.NoError(t, err)
	require.Equal(t, milvusTableTestExternalPrefix+"files/insert_log/1/2/10/_data/1.parquet", got)

	got, err = resolve(milvusTableTestExternalPrefix + "files/insert_log/1/2/10/_data/1.parquet")
	require.NoError(t, err)
	require.Equal(t, milvusTableTestExternalPrefix+"files/insert_log/1/2/10/_data/1.parquet", got)

	for _, path := range []string{
		"/var/lib/milvus/data/insert_log/1/2/10/_data/1.parquet",
		// Not a URI to milvus-storage, so it would be read as a path.
		"s3://storage:9000/snapshots/files/insert_log/1/2/10/_data/1 2.parquet",
	} {
		_, err := resolve(path)
		require.ErrorIs(t, err, ErrLoonPermanent, path)
		require.ErrorContains(t, err, "external source storage", path)
	}
}

func TestMilvusTableSourceManifestOutsideExternalSourceIsNotRead(t *testing.T) {
	config := manifestTestStorageConfig(t)
	dir := config.GetRootPath()
	// A real StorageV3 manifest in this cluster's storage.
	source := createBaseManifest(t, filepath.Join(dir, "insert_log/1/2/10"), config)
	target := filepath.Join(dir, "insert_log/3/4/30")

	_, err := CreateMilvusTableManifestFromSegmentManifests(target, []string{"100"},
		[]Fragment{{FilePath: source, EndRow: 1, RowCount: 1}}, config, milvusTableTestExtfs)
	require.ErrorIs(t, err, ErrLoonPermanent)
	require.ErrorContains(t, err, "external source storage")

	deltalogs, err := GetMilvusTableSourceDeltalogs(source, config, milvusTableTestExtfs, []string{"100"})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Nil(t, deltalogs)

	reader, err := NewMilvusTableSourceFFIPackedReader(source, nil, []string{"100"}, DefaultReadBufferSize, config, nil, milvusTableTestExtfs)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Nil(t, reader)

	// Without an external source, source manifests are in this cluster's storage.
	_, err = CreateMilvusTableManifestFromSegmentManifests(target, []string{"pk", "ts"},
		[]Fragment{{FilePath: source, EndRow: 1, RowCount: 1}}, config, ExternalSpecContext{})
	require.NoError(t, err)
}

func TestCheckMilvusTableSourceDeltalogPaths(t *testing.T) {
	config := manifestTestStorageConfig(t)
	check := func(path string) error {
		return CheckMilvusTableSourceDeltalogPaths([]*datapb.FieldBinlog{{
			FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: path, EntriesNum: 1}},
		}}, config, milvusTableTestExtfs)
	}
	require.NoError(t, check("files/insert_log/1/2/10/_delta/7"))
	require.NoError(t, check(milvusTableTestExternalPrefix+"files/insert_log/1/2/10/_delta/7"))
	require.ErrorIs(t, check("/var/lib/milvus/data/insert_log/1/2/10/_delta/7"), merr.ErrParameterInvalid)
	// Without an external source, paths are not checked.
	require.NoError(t, CheckMilvusTableSourceDeltalogPaths([]*datapb.FieldBinlog{{
		FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "/var/lib/milvus/data/delta_log/7", EntriesNum: 1}},
	}}, config, ExternalSpecContext{}))
}
