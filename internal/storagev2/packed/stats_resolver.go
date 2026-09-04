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
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// compoundStatsLogIdx is the log index that identifies compound stats format.
// This mirrors storage.CompoundStatsType.LogIdx() == "1" but avoids
// importing internal/storage (which already imports this package).
const compoundStatsLogIdx = "1"

// StatsResolver resolves stat file paths from either a LOON manifest (V3)
// or legacy FieldBinlog arrays (V2). It caches the manifest FFI call
// so multiple stat lookups share a single read.
type StatsResolver struct {
	// Manifest-based (V3)
	manifestPath  string
	storageConfig *indexpb.StorageConfig

	// Legacy (V2)
	statslogs     []*datapb.FieldBinlog
	bm25Logs      []*datapb.FieldBinlog
	textStatsLogs map[int64]*datapb.TextIndexStats
	jsonKeyStats  map[int64]*datapb.JsonKeyStats

	// Lazy-loaded manifest cache
	manifestStats  map[string]ManifestStat
	manifestLoaded bool
	manifestErr    error
}

// NewStatsResolver creates a StatsResolver. Pass a non-empty manifestPath
// for V3 (manifest-based) segments, or an empty string for V2 (legacy).
func NewStatsResolver(manifestPath string, storageConfig *indexpb.StorageConfig) *StatsResolver {
	return &StatsResolver{
		manifestPath:  manifestPath,
		storageConfig: storageConfig,
	}
}

// NewStatsResolverFromLoadInfo creates a fully-populated StatsResolver from a
// SegmentLoadInfo. This is the preferred constructor for QueryNode call sites.
func NewStatsResolverFromLoadInfo(loadInfo *querypb.SegmentLoadInfo) *StatsResolver {
	return &StatsResolver{
		manifestPath:  loadInfo.GetManifestPath(),
		storageConfig: CreateStorageConfig(),
		statslogs:     loadInfo.GetStatslogs(),
		bm25Logs:      loadInfo.GetBm25Logs(),
		textStatsLogs: loadInfo.GetTextStatsLogs(),
		jsonKeyStats:  loadInfo.GetJsonKeyStatsLogs(),
	}
}

// NewStatsResolverFromSegmentInfo creates a fully-populated StatsResolver from a
// datapb.SegmentInfo. This is the preferred constructor for DataNode call sites.
func NewStatsResolverFromSegmentInfo(info *datapb.SegmentInfo) *StatsResolver {
	return &StatsResolver{
		manifestPath:  info.GetManifestPath(),
		storageConfig: CreateStorageConfig(),
		statslogs:     info.GetStatslogs(),
		bm25Logs:      info.GetBm25Statslogs(),
		textStatsLogs: info.GetTextStatsLogs(),
		jsonKeyStats:  info.GetJsonKeyStats(),
	}
}

func (r *StatsResolver) WithStatslogs(s []*datapb.FieldBinlog) *StatsResolver {
	r.statslogs = s
	return r
}

func (r *StatsResolver) WithBM25Logs(b []*datapb.FieldBinlog) *StatsResolver {
	r.bm25Logs = b
	return r
}

func (r *StatsResolver) WithTextStatsLogs(t map[int64]*datapb.TextIndexStats) *StatsResolver {
	r.textStatsLogs = t
	return r
}

func (r *StatsResolver) WithJSONKeyStats(j map[int64]*datapb.JsonKeyStats) *StatsResolver {
	r.jsonKeyStats = j
	return r
}

// isManifest returns true when stats come from a LOON manifest.
func (r *StatsResolver) isManifest() bool {
	return r.manifestPath != ""
}

// BloomFilterPaths returns bloom filter file paths for a segment.
// Compound stats format is handled transparently — if a compound stats file
// is found, only that single path is returned.
func (r *StatsResolver) BloomFilterPaths(pkFieldID int64) ([]string, error) {
	if !r.isManifest() {
		return filterPKStatsBinlogs(r.statslogs, pkFieldID), nil
	}

	if err := r.loadManifest(); err != nil {
		return nil, err
	}

	key := fmt.Sprintf("bloom_filter.%d", pkFieldID)
	stat, ok := r.manifestStats[key]
	if !ok || len(stat.Paths) == 0 {
		return nil, nil
	}

	resolved := r.loonStatPaths(stat.Paths)
	for i, p := range stat.Paths {
		_, logidx := path.Split(p)
		if logidx == compoundStatsLogIdx {
			return []string{resolved[i]}, nil
		}
	}
	return resolved, nil
}

// BloomFilterMemorySize returns the estimated memory size for bloom filters.
// For manifest: reads memory_size metadata. For legacy: sums MemorySize from
// the FieldBinlog matching pkFieldID. Returns 0 if unavailable.
func (r *StatsResolver) BloomFilterMemorySize(pkFieldID int64) (int64, error) {
	if !r.isManifest() {
		var total int64
		for _, fb := range r.statslogs {
			if fb.FieldID == pkFieldID {
				for _, b := range fb.GetBinlogs() {
					total += b.GetMemorySize()
				}
			}
		}
		return total, nil
	}

	if err := r.loadManifest(); err != nil {
		return 0, err
	}

	key := fmt.Sprintf("bloom_filter.%d", pkFieldID)
	stat, ok := r.manifestStats[key]
	if !ok {
		return 0, nil
	}

	memStr, ok := stat.Metadata["memory_size"]
	if !ok || memStr == "" {
		return 0, nil
	}

	memSize, err := strconv.ParseInt(memStr, 10, 64)
	if err != nil {
		return 0, nil
	}
	return memSize, nil
}

// ChunkManagerBloomFilterPaths returns bloom-filter paths in ChunkManager's
// complete-key namespace. Callers that read through loon or an external-spec
// filesystem must use BloomFilterPaths instead.
func (r *StatsResolver) ChunkManagerBloomFilterPaths(pkFieldID int64) ([]string, error) {
	paths, err := r.BloomFilterPaths(pkFieldID)
	if err != nil {
		return nil, err
	}
	return r.chunkManagerStatPaths(paths)
}

// BM25StatsPaths returns BM25 stat file paths grouped by field ID.
func (r *StatsResolver) BM25StatsPaths() (map[int64][]string, error) {
	if !r.isManifest() {
		return filterBM25Stats(r.bm25Logs), nil
	}

	if err := r.loadManifest(); err != nil {
		return nil, err
	}

	result := make(map[int64][]string)
	for key, stat := range r.manifestStats {
		prefix, fieldID, ok := ParseStatKey(key)
		if !ok || prefix != "bm25" || len(stat.Paths) == 0 {
			continue
		}

		resolved := r.loonStatPaths(stat.Paths)

		found := false
		for i, p := range stat.Paths {
			_, logidx := path.Split(p)
			if logidx == compoundStatsLogIdx {
				result[fieldID] = []string{resolved[i]}
				found = true
				break
			}
		}
		if !found {
			result[fieldID] = resolved
		}
	}
	return result, nil
}

// ChunkManagerBM25StatsPaths returns BM25 paths in ChunkManager's complete-key
// namespace. Metadata-only and loon consumers must use BM25StatsPaths instead.
func (r *StatsResolver) ChunkManagerBM25StatsPaths() (map[int64][]string, error) {
	pathsByField, err := r.BM25StatsPaths()
	if err != nil {
		return nil, err
	}
	result := make(map[int64][]string, len(pathsByField))
	for fieldID, paths := range pathsByField {
		result[fieldID], err = r.chunkManagerStatPaths(paths)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// StatsResult holds stats info together with the base paths for each field.
type StatsResult struct {
	TextIndexStats map[int64]*datapb.TextIndexStats
	JSONKeyStats   map[int64]*datapb.JsonKeyStats
	TextBasePaths  map[int64]string // fieldID -> basePath for text index
	JSONBasePaths  map[int64]string // fieldID -> basePath for json key stats
}

// TextAndJSONIndexStats returns text index and JSON key stats.
// For manifest: parsed from manifest metadata (highest version wins per field).
// For legacy: returns the WithTextStatsLogs/WithJSONKeyStats maps directly.
func (r *StatsResolver) TextAndJSONIndexStats() (
	map[int64]*datapb.TextIndexStats, map[int64]*datapb.JsonKeyStats, error,
) {
	result := r.TextAndJSONIndexStatsWithBasePaths()
	return result.TextIndexStats, result.JSONKeyStats, result.err
}

// TextAndJSONIndexStatsWithBasePaths returns stats with base path information.
// For V3 (manifest): basePaths are extracted from the manifest stat paths.
// For V2 (legacy): basePaths are empty (backward compat).
func (r *StatsResolver) TextAndJSONIndexStatsWithBasePaths() *StatsResultWithErr {
	if !r.isManifest() {
		return &StatsResultWithErr{
			StatsResult: StatsResult{
				TextIndexStats: r.textStatsLogs,
				JSONKeyStats:   r.jsonKeyStats,
			},
		}
	}

	if err := r.loadManifest(); err != nil {
		return &StatsResultWithErr{err: err}
	}

	textIndexedInfo := make(map[int64]*datapb.TextIndexStats)
	jsonKeyIndexInfo := make(map[int64]*datapb.JsonKeyStats)
	textBasePaths := make(map[int64]string)
	jsonBasePaths := make(map[int64]string)

	basePath, _, _ := UnmarshalManifestPath(r.manifestPath)

	for key, stat := range r.manifestStats {
		prefix, fieldID, ok := ParseStatKey(key)
		if !ok {
			continue
		}

		switch prefix {
		case "text_index":
			// For V3: extract basePath and convert to relative paths.
			statBasePath := basePath + "/_stats/" + key
			resolvedPaths := r.loonStatPaths(stat.Paths)
			relativeFiles := stripBasePathPrefix(resolvedPaths, statBasePath)
			// Unified text indexes are opened through FileManager, which resolves
			// the remote object from StatsBasePath plus the file basename. A manifest
			// may place the object under attempt directories such as taskID/version,
			// so use the actual object directory as StatsBasePath. This also accepts
			// older writers that keep the file directly under the field directory.
			if len(resolvedPaths) == 1 && strings.HasSuffix(resolvedPaths[0], ".v3") {
				statBasePath = path.Dir(resolvedPaths[0])
				relativeFiles = []string{path.Base(resolvedPaths[0])}
			}

			version, _ := strconv.ParseInt(stat.Metadata["version"], 10, 64)
			buildID, _ := strconv.ParseInt(stat.Metadata["build_id"], 10, 64)
			logSize, _ := strconv.ParseInt(stat.Metadata["log_size"], 10, 64)
			memorySize, _ := strconv.ParseInt(stat.Metadata["memory_size"], 10, 64)
			scalarVer, _ := strconv.ParseInt(stat.Metadata["current_scalar_index_version"], 10, 32)

			textStats := &datapb.TextIndexStats{
				FieldID:                   fieldID,
				Version:                   version,
				BuildID:                   buildID,
				Files:                     relativeFiles,
				LogSize:                   logSize,
				MemorySize:                memorySize,
				CurrentScalarIndexVersion: int32(scalarVer),
			}

			existing, ok := textIndexedInfo[fieldID]
			if !ok || version > existing.GetVersion() {
				textIndexedInfo[fieldID] = textStats
				textBasePaths[fieldID] = statBasePath
			}

		case "json_stats":
			if _, ok := r.jsonKeyStats[fieldID]; !ok {
				continue
			}
			// For V3: extract basePath and convert to relative paths
			statBasePath := basePath + "/_stats/" + key
			resolvedPaths := r.loonStatPaths(stat.Paths)
			relativeFiles := stripBasePathPrefix(resolvedPaths, statBasePath)

			version, _ := strconv.ParseInt(stat.Metadata["version"], 10, 64)
			buildID, _ := strconv.ParseInt(stat.Metadata["build_id"], 10, 64)
			logSize, _ := strconv.ParseInt(stat.Metadata["log_size"], 10, 64)
			memorySize, _ := strconv.ParseInt(stat.Metadata["memory_size"], 10, 64)
			dataFormat, _ := strconv.ParseInt(stat.Metadata["json_key_stats_data_format"], 10, 64)

			jsonStats := &datapb.JsonKeyStats{
				FieldID:                fieldID,
				Version:                version,
				BuildID:                buildID,
				Files:                  relativeFiles,
				LogSize:                logSize,
				MemorySize:             memorySize,
				JsonKeyStatsDataFormat: dataFormat,
			}

			existing, ok := jsonKeyIndexInfo[fieldID]
			if !ok || version > existing.GetVersion() {
				jsonKeyIndexInfo[fieldID] = jsonStats
				jsonBasePaths[fieldID] = statBasePath
			}
		}
	}

	return &StatsResultWithErr{
		StatsResult: StatsResult{
			TextIndexStats: textIndexedInfo,
			JSONKeyStats:   jsonKeyIndexInfo,
			TextBasePaths:  textBasePaths,
			JSONBasePaths:  jsonBasePaths,
		},
	}
}

// StatsResultWithErr wraps StatsResult with an error.
type StatsResultWithErr struct {
	StatsResult
	err error
}

// Err returns the error from loading stats.
func (r *StatsResultWithErr) Err() error {
	return r.err
}

// stripBasePathPrefix strips the basePath prefix from absolute paths to get relative paths.
// Paths that don't match the expected prefix are left unchanged.
// at a parent directory level that don't belong to this stat entry).
func stripBasePathPrefix(paths []string, basePath string) []string {
	prefix := basePath + "/"
	result := make([]string, 0, len(paths))
	for _, p := range paths {
		if strings.HasPrefix(p, prefix) {
			result = append(result, p[len(prefix):])
		} else {
			result = append(result, p)
		}
	}
	return result
}

// loadManifest lazily loads and caches the manifest stats via FFI.
func (r *StatsResolver) loadManifest() error {
	if r.manifestLoaded {
		return r.manifestErr
	}
	r.manifestLoaded = true

	stats, err := GetManifestStats(r.manifestPath, r.storageConfig)
	if err != nil {
		r.manifestErr = merr.Wrap(err, "failed to get manifest stats")
		return r.manifestErr
	}
	r.manifestStats = stats
	return nil
}

// loonStatPaths returns paths expanded against the manifest base. The
// resulting namespace is still owned by loon: remote paths are complete object
// keys, while local paths remain relative to the configured filesystem root.
// This form is used by text/JSON consumers that pass the paths back to loon.
func (r *StatsResolver) loonStatPaths(paths []string) []string {
	return paths
}

// chunkManagerStatPaths converts manifest-expanded bloom/BM25 paths to the
// complete-key contract required by ChunkManager. Remote manifest paths are
// already complete object keys. A local loon manifest is rooted at
// localStorage.path and therefore returns root-relative paths, which must be
// made into complete filesystem paths before Go storage I/O.
//
// Text/JSON index paths deliberately do not use this conversion: those paths
// are passed back through loon/segcore, whose local filesystem applies the
// configured root itself.
func (r *StatsResolver) chunkManagerStatPaths(paths []string) ([]string, error) {
	for _, filePath := range paths {
		if strings.TrimSpace(filePath) == "" || strings.Contains(filePath, "://") {
			return nil, merr.WrapErrDataIntegrityMsg("invalid ChunkManager stats path")
		}
	}
	if r.storageConfig.GetStorageType() != "local" {
		rootPath := strings.Trim(path.Clean(r.storageConfig.GetRootPath()), "/")
		for _, filePath := range paths {
			rawPath := strings.TrimSpace(filePath)
			cleanPath := path.Clean(rawPath)
			if path.IsAbs(filePath) || cleanPath == "." || cleanPath == ".." || strings.HasPrefix(cleanPath, "../") {
				return nil, merr.WrapErrDataIntegrityMsg("remote ChunkManager stats path must be a bucket-relative object key")
			}
			if cleanPath != rawPath {
				return nil, merr.WrapErrDataIntegrityMsg("remote ChunkManager stats path must be a canonical object key")
			}
			if rootPath != "" && rootPath != "." && cleanPath != rootPath && !strings.HasPrefix(cleanPath, rootPath+"/") {
				return nil, merr.WrapErrDataIntegrityMsg("remote ChunkManager stats path is outside the configured storage root")
			}
		}
		return paths, nil
	}

	rootPath := filepath.Clean(r.storageConfig.GetRootPath())
	if !filepath.IsAbs(rootPath) {
		return nil, merr.WrapErrDataIntegrityMsg("local storage root must be an absolute filesystem path")
	}
	result := make([]string, len(paths))
	for i, filePath := range paths {
		completePath := filepath.Clean(filepath.FromSlash(filePath))
		if !filepath.IsAbs(completePath) {
			completePath = filepath.Join(rootPath, completePath)
		}
		relativePath, err := filepath.Rel(rootPath, completePath)
		if err != nil || relativePath == "." || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
			return nil, merr.WrapErrDataIntegrityMsg("local ChunkManager stats path is outside the configured storage root")
		}
		result[i] = completePath
	}
	return result, nil
}

// ParseStatKey parses a "type.fieldID" stat key into its type prefix and field ID.
func ParseStatKey(key string) (string, int64, bool) {
	idx := strings.LastIndex(key, ".")
	if idx < 0 {
		return "", 0, false
	}
	prefix := key[:idx]
	fieldID, err := strconv.ParseInt(key[idx+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return prefix, fieldID, true
}

// filterPKStatsBinlogs filters legacy FieldBinlog arrays for the given pkFieldID.
// If a compound stats file is found, only that single path is returned.
func filterPKStatsBinlogs(fieldBinlogs []*datapb.FieldBinlog, pkFieldID int64) []string {
	result := make([]string, 0)
	for _, fieldBinlog := range fieldBinlogs {
		if fieldBinlog.FieldID == pkFieldID {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				_, logidx := path.Split(binlog.GetLogPath())
				if logidx == compoundStatsLogIdx {
					return []string{binlog.GetLogPath()}
				}
				result = append(result, binlog.GetLogPath())
			}
		}
	}
	return result
}

// filterBM25Stats filters legacy FieldBinlog arrays into BM25 paths grouped by field ID.
func filterBM25Stats(fieldBinlogs []*datapb.FieldBinlog) map[int64][]string {
	result := make(map[int64][]string, 0)
	for _, fieldBinlog := range fieldBinlogs {
		logpaths := []string{}
		for _, binlog := range fieldBinlog.GetBinlogs() {
			_, logidx := path.Split(binlog.GetLogPath())
			if logidx == compoundStatsLogIdx {
				logpaths = []string{binlog.GetLogPath()}
				break
			}
			logpaths = append(logpaths, binlog.GetLogPath())
		}
		result[fieldBinlog.FieldID] = logpaths
	}
	return result
}
