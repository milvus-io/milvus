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

package meta

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// CompactManifestStats detaches the file lists supplied with a newly published
// manifest. Field entries remain available for scheduling and load estimates.
func CompactManifestStats(segment *datapb.SegmentInfo) {
	if segment.GetStorageVersion() == storage.StorageV3 && segment.GetManifestPath() != "" {
		compactStatsFiles(segment, nil)
	}
}

type fieldStats interface {
	GetFiles() []string
	GetBuildID() int64
	GetVersion() int64
	GetFilesInManifest() bool
}

func hasInlineStatsFiles[T fieldStats](fields map[int64]T) bool {
	for _, stats := range fields {
		if len(stats.GetFiles()) > 0 {
			return true
		}
	}
	return false
}

func needsManifestStatsFiles[T fieldStats](fields map[int64]T) bool {
	for _, stats := range fields {
		if stats.GetFilesInManifest() && len(stats.GetFiles()) == 0 {
			return true
		}
	}
	return false
}

// A catalog row from an older writer may contain files that never reached the
// manifest. Compare build and version before releasing an unmarked list.
func compactCatalogStats(segment *datapb.SegmentInfo, entries map[string]packed.ManifestStat) {
	compactStatsFiles(segment, func(key string, s fieldStats) bool {
		stat := entries[key]
		return len(stat.Paths) > 0 && stat.Metadata["build_id"] == strconv.FormatInt(s.GetBuildID(), 10) &&
			stat.Metadata["version"] == strconv.FormatInt(s.GetVersion(), 10)
	})
}

// CompactCopiedStats checks older snapshot placeholders against the files in the
// adopted target revision. Restore may assign new build IDs and object roots,
// while preserving the paths below the per-field stats directory.
func CompactCopiedStats(segment *datapb.SegmentInfo) {
	if segment.GetStorageVersion() != storage.StorageV3 || segment.GetManifestPath() == "" ||
		(!hasInlineStatsFiles(segment.TextStatsLogs) && !hasInlineStatsFiles(segment.JsonKeyStats)) {
		return
	}
	entries, err := packed.GetManifestStats(segment.GetManifestPath(), CreateStorageConfig())
	if err != nil {
		return // Keep the original lists until their ownership can be verified.
	}
	compactStatsFiles(segment, func(key string, s fieldStats) bool {
		stat := entries[key]
		if len(stat.Paths) == 0 || stat.Metadata["version"] != strconv.FormatInt(s.GetVersion(), 10) {
			return false
		}
		suffix := "/_stats/" + key + "/"
		relative := func(paths []string) []string {
			names := make([]string, 0, len(paths))
			for _, name := range paths {
				if i := strings.LastIndex(name, suffix); i >= 0 {
					name = name[i+len(suffix):]
				}
				names = append(names, name)
			}
			slices.Sort(names)
			return names
		}
		return slices.Equal(relative(stat.Paths), relative(s.GetFiles()))
	})
}

func compactStatsFiles(segment *datapb.SegmentInfo, owns func(string, fieldStats) bool) {
	canDrop := func(kind string, id int64, s fieldStats) bool {
		return len(s.GetFiles()) > 0 && (owns == nil || owns(fmt.Sprintf("%s.%d", kind, id), s))
	}
	if hasInlineStatsFiles(segment.TextStatsLogs) {
		segment.TextStatsLogs = maps.Clone(segment.TextStatsLogs)
		for id, s := range segment.TextStatsLogs {
			if canDrop("text_index", id, s) {
				cloned := proto.Clone(s).(*datapb.TextIndexStats)
				cloned.Files = nil
				cloned.FilesInManifest = true
				segment.TextStatsLogs[id] = cloned
			}
		}
	}
	if hasInlineStatsFiles(segment.JsonKeyStats) {
		segment.JsonKeyStats = maps.Clone(segment.JsonKeyStats)
		for id, s := range segment.JsonKeyStats {
			if canDrop("json_stats", id, s) {
				cloned := proto.Clone(s).(*datapb.JsonKeyStats)
				cloned.Files = nil
				cloned.FilesInManifest = true
				segment.JsonKeyStats[id] = cloned
			}
		}
	}
}

// ResolveSegmentStatsFiles fills file lists on an RPC or snapshot copy. Use a
// single read of that copy's immutable revision for all of its missing lists.
func ResolveSegmentStatsFiles(segment *datapb.SegmentInfo) error {
	if segment.GetStorageVersion() != storage.StorageV3 || segment.GetManifestPath() == "" ||
		(!needsManifestStatsFiles(segment.TextStatsLogs) && !needsManifestStatsFiles(segment.JsonKeyStats)) {
		return nil
	}
	manifest, err := packed.GetManifestStats(segment.GetManifestPath(), CreateStorageConfig())
	if err != nil {
		return merr.Wrap(err, "read segment stats files")
	}
	base, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return err
	}
	files := func(kind string, id int64) ([]string, error) {
		key := fmt.Sprintf("%s.%d", kind, id)
		entry, ok := manifest[key]
		if !ok || len(entry.Paths) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("segment %d manifest %s is missing files for stat %s", segment.GetID(), segment.GetManifestPath(), key)
		}
		return entry.Paths, nil
	}
	for id, s := range segment.TextStatsLogs {
		if s.GetFilesInManifest() && len(s.GetFiles()) == 0 {
			paths, err := files("text_index", id)
			if err != nil {
				return err
			}
			value := proto.Clone(s).(*datapb.TextIndexStats)
			value.Files = paths
			segment.TextStatsLogs[id] = value
		}
	}
	for id, s := range segment.JsonKeyStats {
		if s.GetFilesInManifest() && len(s.GetFiles()) == 0 {
			paths, err := files("json_stats", id)
			if err != nil {
				return err
			}
			prefix := metautil.BuildJSONKeyStatsV3Prefix(base, id) + "/"
			value := proto.Clone(s).(*datapb.JsonKeyStats)
			for _, file := range paths {
				value.Files = append(value.Files, strings.TrimPrefix(file, prefix))
			}
			segment.JsonKeyStats[id] = value
		}
	}
	return nil
}
