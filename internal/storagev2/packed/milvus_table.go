// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"encoding/json"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/snapshotio"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const milvusTableExploreManifestFile = "milvus-table-explore.json"

var errMilvusTableStorageV2ManifestListMissing = errors.New(
	"milvus-table requires storagev2_manifest_list in snapshot metadata; " +
		"create the source snapshot from a StorageV3 collection " +
		"(enable common.storage.useLoonFFI=true before writing source data)",
)

func IsMilvusTableStorageV2ManifestListMissing(err error) bool {
	return errors.Is(err, errMilvusTableStorageV2ManifestListMissing)
}

// milvusTableExploreManifest is the Go-side explore result persisted for
// milvus-table sources. Its FileInfo.NumRows stores source segment row counts,
// not parquet file row counts.
type milvusTableExploreManifest struct {
	Format string     `json:"format"`
	Files  []FileInfo `json:"files"`
}

func isMilvusTableFormat(format string) bool {
	return format == externalspec.FormatMilvusTable
}

// IsMilvusTableStorageV3DeltalogPath reports whether the path points to a
// StorageV3 packed deltalog under a segment manifest base path.
func IsMilvusTableStorageV3DeltalogPath(sourcePath string) bool {
	return strings.Contains(sourcePath, "/_delta/") || strings.HasPrefix(sourcePath, "_delta/")
}

// IsMilvusTableLegacyL0DeltalogPath reports whether the path points to a
// legacy L0 deltalog produced by the streaming delete flush path.
func IsMilvusTableLegacyL0DeltalogPath(sourcePath string) bool {
	return strings.Contains(sourcePath, "/delta_log/") || strings.HasPrefix(sourcePath, "delta_log/")
}

// ValidateMilvusTableStorageV3DeltalogPath rejects legacy source deltalogs in
// milvus-table snapshots. The format requires StorageV3 packed deltalogs.
func ValidateMilvusTableStorageV3DeltalogPath(sourcePath string) error {
	if IsMilvusTableStorageV3DeltalogPath(sourcePath) {
		return nil
	}
	return merr.WrapErrServiceInternalMsg("milvus-table only supports StorageV3 source deltalogs under _delta, got %s", sourcePath)
}

// ValidateMilvusTableSourceDeltalogPath accepts the two deltalog shapes a
// StorageV3 milvus-table snapshot can currently expose: StorageV3 segment
// deltas under _delta and legacy L0 delete overlays under delta_log.
func ValidateMilvusTableSourceDeltalogPath(sourcePath string) error {
	if IsMilvusTableStorageV3DeltalogPath(sourcePath) || IsMilvusTableLegacyL0DeltalogPath(sourcePath) {
		return nil
	}
	return merr.WrapErrServiceInternalMsg("milvus-table only supports StorageV3 source deltalogs under _delta or legacy L0 deltalogs under delta_log, got %s", sourcePath)
}

// resolveMilvusTableSnapshotMetadataPath validates the milvus-table external
// spec and returns the snapshot metadata JSON path carried by external_source.
func resolveMilvusTableSnapshotMetadataPath(externalSource, externalSpec string) (string, error) {
	if _, err := externalspec.ParseExternalSpec(externalSpec); err != nil {
		return "", err
	}
	if strings.HasSuffix(strings.ToLower(externalSource), ".json") {
		return externalSource, nil
	}
	return "", merr.WrapErrServiceInternalMsg("milvus-table requires external_source to be a snapshot metadata JSON path")
}

// buildMilvusTableFileInfosFromSnapshotMetadata converts snapshot metadata into
// one FileInfo per source StorageV3 segment manifest. L0 deltalogs are attached
// to every source segment because they are snapshot-level delete overlays.
func buildMilvusTableFileInfosFromSnapshotMetadata(
	metadataBytes []byte,
	getSegmentDescription func(string, int32) (*datapb.SegmentDescription, error),
	resolveManifestPath func(string) (string, error),
) ([]FileInfo, error) {
	metadata, err := snapshotio.ParseSnapshotMetadataWithVersionCheck(metadataBytes)
	if err != nil {
		return nil, merr.Wrap(err, "parse milvus snapshot metadata")
	}
	manifestList := metadata.GetStoragev2ManifestList()
	if len(manifestList) == 0 {
		return nil, errMilvusTableStorageV2ManifestListMissing
	}
	if resolveManifestPath == nil {
		resolveManifestPath = func(manifestPath string) (string, error) {
			return manifestPath, nil
		}
	}

	if getSegmentDescription == nil {
		return nil, merr.WrapErrServiceInternalMsg("milvus-table requires source segment manifests to read source segment row counts")
	}

	sourceSegments := make(map[int64]*datapb.SegmentDescription)
	var l0Deltalogs []*datapb.FieldBinlog
	for _, manifestPath := range metadata.GetManifestList() {
		segment, err := getSegmentDescription(manifestPath, metadata.GetFormatVersion())
		if err != nil {
			return nil, merr.Wrapf(err, "read source segment manifest %s", manifestPath)
		}
		if segment == nil {
			return nil, merr.WrapErrServiceInternalMsg("read source segment manifest %s: empty segment", manifestPath)
		}
		if segment.GetSegmentLevel() == datapb.SegmentLevel_L0 {
			l0Deltalogs = append(l0Deltalogs, segment.GetDeltalogs()...)
			continue
		}
		sourceSegments[segment.GetSegmentId()] = segment
	}

	fileInfos := make([]FileInfo, 0, len(manifestList))
	for _, manifest := range manifestList {
		if manifest.GetManifest() == "" {
			return nil, merr.WrapErrServiceInternalMsg("milvus-table snapshot segment %d has empty storagev2 manifest", manifest.GetSegmentId())
		}
		manifestPath, err := resolveManifestPath(manifest.GetManifest())
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "resolve source segment manifest %s", manifest.GetManifest())
		}
		sourceSegment, ok := sourceSegments[manifest.GetSegmentId()]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("milvus-table snapshot segment %d has storagev2 manifest but no source segment manifest", manifest.GetSegmentId())
		}
		rowCount := sourceSegment.GetNumOfRows()
		if rowCount <= 0 {
			return nil, merr.WrapErrServiceInternalMsg("source segment %d has non-positive row count %d", manifest.GetSegmentId(), rowCount)
		}
		info := FileInfo{
			FilePath:        manifestPath,
			NumRows:         rowCount,
			SourceSegmentID: manifest.GetSegmentId(),
		}
		// Source segment manifest deltas are imported while building the
		// target manifest. Only L0 deltas need Go-side materialization.
		info.Deltalogs = append(info.Deltalogs, l0Deltalogs...)
		fileInfos = append(fileInfos, info)
	}
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].FilePath < fileInfos[j].FilePath
	})
	return fileInfos, nil
}

// resolveMilvusTableSourceManifestPath rewrites a source manifest base path
// through the external-source resolver while preserving the StorageV3 version.
func resolveMilvusTableSourceManifestPath(
	manifestPath string,
	resolveSourcePath func(string) (string, error),
) (string, error) {
	if resolveSourcePath == nil {
		return manifestPath, nil
	}
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", err
	}
	resolvedBasePath, err := resolveSourcePath(basePath)
	if err != nil {
		return "", err
	}
	if resolvedBasePath == basePath {
		return manifestPath, nil
	}
	return MarshalManifestPath(resolvedBasePath, version), nil
}

// resolveMilvusTableSegmentDeltalogPaths rewrites deltalog paths found in a
// source segment manifest to the same resolved external filesystem root.
func resolveMilvusTableSegmentDeltalogPaths(
	segment *datapb.SegmentDescription,
	resolveSourcePath func(string) (string, error),
) error {
	if segment == nil || resolveSourcePath == nil {
		return nil
	}
	for _, fieldBinlog := range segment.GetDeltalogs() {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if binlog.GetLogPath() == "" {
				continue
			}
			resolvedPath, err := resolveSourcePath(binlog.GetLogPath())
			if err != nil {
				return err
			}
			binlog.LogPath = resolvedPath
		}
	}
	return nil
}

// ReadMilvusTableSnapshotMetadata reads and parses the source snapshot metadata
// using external spec filesystem aliases when the source is remote.
func ReadMilvusTableSnapshotMetadata(
	externalSource string,
	externalSpec string,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
) (*datapb.SnapshotMetadata, error) {
	metadataPath, err := resolveMilvusTableSnapshotMetadataPath(externalSource, externalSpec)
	if err != nil {
		return nil, err
	}
	metadataBytes, err := ReadFileWithExternalSpec(
		storageConfig,
		metadataPath,
		milvusTableReadFileExtfs(externalSource, extfs),
	)
	if err != nil {
		return nil, merr.Wrap(err, "read milvus snapshot metadata")
	}
	metadata, err := snapshotio.ParseSnapshotMetadataWithVersionCheck(metadataBytes)
	if err != nil {
		return nil, merr.Wrap(err, "parse milvus snapshot metadata")
	}
	return metadata, nil
}

// IsRemoteObjectURL reports whether s is a fully-qualified object-store URL
// (has both scheme and host), e.g. "s3://bucket/key". A bare path or an
// unparseable string is treated as local (false). Callers that need to surface
// a parse error must not use this helper.
func IsRemoteObjectURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && u.Scheme != "" && u.Host != ""
}

func milvusTableReadFileExtfs(externalSource string, extfs ExternalSpecContext) ExternalSpecContext {
	if !IsRemoteObjectURL(externalSource) {
		extfs.Source = ""
	}
	return extfs
}

// writeMilvusTableExploreManifest persists the source manifest list that
// DataCoord will later split across refresh tasks.
func writeMilvusTableExploreManifest(
	baseDir string,
	fileInfos []FileInfo,
	storageConfig *indexpb.StorageConfig,
) (string, error) {
	manifestPath := path.Join(baseDir, milvusTableExploreManifestFile)
	data, err := json.Marshal(milvusTableExploreManifest{
		Format: externalspec.FormatMilvusTable,
		Files:  fileInfos,
	})
	if err != nil {
		return "", err
	}
	if err := WriteFile(storageConfig, manifestPath, data); err != nil {
		return "", err
	}
	return manifestPath, nil
}

// readMilvusTableExploreManifest reads the persisted milvus-table explore
// result produced by writeMilvusTableExploreManifest.
func readMilvusTableExploreManifest(manifestPath string, storageConfig *indexpb.StorageConfig) ([]FileInfo, error) {
	data, err := ReadFile(storageConfig, manifestPath)
	if err != nil {
		return nil, err
	}
	var manifest milvusTableExploreManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "parse milvus-table explore manifest")
	}
	if manifest.Format != externalspec.FormatMilvusTable {
		return nil, merr.WrapErrServiceInternalMsg("unexpected milvus-table explore manifest format %q", manifest.Format)
	}
	return manifest.Files, nil
}

// readMilvusSnapshotSegmentManifest reads one source segment manifest and
// includes the manifest path in parse errors for diagnostics.
func readMilvusSnapshotSegmentManifest(
	manifestPath string,
	formatVersion int32,
	readFile func(string) ([]byte, error),
) (*datapb.SegmentDescription, error) {
	data, err := readFile(manifestPath)
	if err != nil {
		return nil, err
	}
	segment, err := snapshotio.ParseSegmentManifest(data, int(formatVersion))
	if err != nil {
		return nil, merr.Wrapf(err, "parse source segment manifest %s", manifestPath)
	}
	return segment, nil
}
