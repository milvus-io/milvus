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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/iskorotkov/avro/v2/ocf"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type ManifestLocatorV3 struct {
	BasePath string `json:"base_path"`
	Version  int64  `json:"ver"`
}

func (locator ManifestLocatorV3) ObjectPath() string {
	return path.Join(locator.BasePath, "_metadata", fmt.Sprintf("manifest-%d.avro", locator.Version))
}

func ParseManifestLocatorV3(raw string) (ManifestLocatorV3, error) {
	var locator ManifestLocatorV3
	decoder := json.NewDecoder(strings.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return locator, errors.New("invalid manifest locator object")
	}
	seen := make(map[string]bool)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return locator, errors.New("invalid manifest locator field")
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return locator, errors.New("duplicate manifest locator field")
		}
		seen[name] = true
		switch name {
		case "base_path":
			err = decoder.Decode(&locator.BasePath)
		case "ver":
			err = decoder.Decode(&locator.Version)
		default:
			return locator, errors.New("unclassified manifest locator field")
		}
		if err != nil {
			return locator, errors.New("invalid manifest locator value")
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return locator, errors.New("incomplete manifest locator")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return locator, errors.New("trailing manifest locator data")
	}
	if !structuralPath(locator.BasePath) || locator.Version <= 0 {
		return locator, errors.New("invalid manifest base path or revision")
	}
	return locator, nil
}

type ManifestReferenceV3 struct {
	CollectionID int64
	SegmentID    int64
	Rows         int64
	Locator      ManifestLocatorV3
}

func LocateManifestsV3(segments []*datapb.SegmentInfo, collectionID int64) ([]ManifestReferenceV3, error) {
	if len(segments) == 0 || collectionID <= 0 {
		return nil, errors.New("no current storage v3 segments")
	}
	references := make([]ManifestReferenceV3, 0, len(segments))
	seen := make(map[int64]bool)
	paths := make(map[string]bool)
	for _, segment := range segments {
		if segment.GetID() <= 0 || seen[segment.GetID()] || segment.GetCollectionID() != collectionID ||
			segment.GetStorageVersion() != 3 || segment.GetNumOfRows() <= 0 {
			return nil, errors.New("invalid or duplicate storage v3 segment")
		}
		locator, err := ParseManifestLocatorV3(segment.GetManifestPath())
		if err != nil {
			return nil, err
		}
		if paths[locator.ObjectPath()] {
			return nil, errors.New("multiple segments reference the same manifest")
		}
		seen[segment.GetID()], paths[locator.ObjectPath()] = true, true
		references = append(references, ManifestReferenceV3{collectionID, segment.GetID(), segment.GetNumOfRows(), locator})
	}
	return references, nil
}

type manifestV3 struct {
	ColumnGroups []manifestV3ColumnGroup `json:"column_groups"`
	DeltaLogs    []struct {
		Path       string `json:"path"`
		Type       int32  `json:"type"`
		NumEntries int64  `json:"num_entries"`
	} `json:"delta_logs"`
	Stats map[string]struct {
		Paths    []string          `json:"paths"`
		Metadata map[string]string `json:"metadata"`
	} `json:"stats"`
	Indexes []struct {
		ColumnName                string            `json:"column_name"`
		IndexName                 string            `json:"index_name"`
		IndexType                 string            `json:"index_type"`
		Path                      string            `json:"path"`
		FieldID                   int64             `json:"field_id"`
		IndexID                   int64             `json:"index_id"`
		BuildID                   int64             `json:"build_id"`
		IndexVersion              int64             `json:"index_version"`
		NumRows                   int64             `json:"num_rows"`
		SerializedSize            int64             `json:"serialized_size"`
		MemSize                   int64             `json:"mem_size"`
		CurrentIndexVersion       int32             `json:"current_index_version"`
		CurrentScalarIndexVersion int32             `json:"current_scalar_index_version"`
		IndexStorePathVersion     int32             `json:"index_store_path_version"`
		IndexFileKeys             []string          `json:"index_file_keys"`
		Properties                map[string]string `json:"properties"`
	} `json:"indexes"`
	LOBFiles []struct {
		Path          string `json:"path"`
		FieldID       int64  `json:"field_id"`
		TotalRows     int64  `json:"total_rows"`
		ValidRows     int64  `json:"valid_rows"`
		FileSizeBytes int64  `json:"file_size_bytes"`
	} `json:"lob_files"`
}

type manifestV3ColumnGroup struct {
	Columns []string         `json:"columns"`
	Format  string           `json:"format"`
	Files   []manifestV3File `json:"files"`
}

type manifestV3File struct {
	Path       string            `json:"path"`
	Start      int64             `json:"start_index"`
	End        int64             `json:"end_index"`
	Properties map[string]string `json:"properties"`
}

type ParquetObjectV3 struct {
	Path    string
	Columns []string
	Start   int64
	End     int64
	Rows    int64
}

// ParseParquetObjectsV3 independently decodes and validates an exact manifest,
// returning every physical raw-data object resolved against its base path.
func ParseParquetObjectsV3(raw []byte, basePath string) ([]ParquetObjectV3, error) {
	if !structuralPath(basePath) {
		return nil, errors.New("invalid manifest base path")
	}
	decoder, err := ocf.NewDecoder(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	for key := range decoder.Metadata() {
		if key != "avro.schema" && key != "avro.codec" {
			return nil, errors.New("unclassified manifest OCF metadata")
		}
	}
	if !decoder.HasNext() {
		return nil, errors.New("manifest has no record")
	}
	var record map[string]any
	if err := decoder.Decode(&record); err != nil {
		return nil, err
	}
	if decoder.HasNext() {
		return nil, errors.New("manifest has multiple records")
	}
	if err := decoder.Error(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	var manifest manifestV3
	strict := json.NewDecoder(bytes.NewReader(encoded))
	strict.DisallowUnknownFields()
	if err := strict.Decode(&manifest); err != nil {
		// Do not echo unexpected field names or values: they may contain
		// precisely the payload or secret that this assertion rejects.
		return nil, errors.New("manifest contains unclassified fields or invalid structural values")
	}
	return manifest.parquetObjects(basePath)
}

func structuralPath(value string) bool {
	return value != "" && value != "." && path.Clean(value) == value &&
		value != ".." && !strings.HasPrefix(value, "../") &&
		!strings.ContainsAny(value, "\\\x00") && strings.IndexFunc(value, unicode.IsControl) < 0
}

func numericMetadata(properties map[string]string, allowed ...string) error {
	for key, value := range properties {
		known := false
		for _, name := range allowed {
			known = known || key == name
		}
		if !known {
			return errors.New("unclassified manifest property")
		}
		if number, err := strconv.ParseInt(value, 10, 64); err != nil || number < 0 {
			return errors.New("invalid manifest numeric property")
		}
	}
	return nil
}

func (manifest *manifestV3) parquetObjects(basePath string) ([]ParquetObjectV3, error) {
	if len(manifest.ColumnGroups) == 0 {
		return nil, errors.New("manifest has no column groups")
	}
	var objects []ParquetObjectV3
	seen := make(map[string]bool)
	columns := make(map[string]bool)
	for _, group := range manifest.ColumnGroups {
		if len(group.Columns) == 0 || len(group.Files) == 0 || group.Format != "parquet" {
			return nil, errors.New("manifest has empty column group or unclassified format")
		}
		for _, column := range group.Columns {
			if id, err := strconv.ParseInt(column, 10, 64); err != nil || id < 0 || columns[column] {
				return nil, errors.New("invalid or duplicate manifest column")
			}
			columns[column] = true
		}
		for _, file := range group.Files {
			if !structuralPath(file.Path) || file.Start < 0 || file.End <= file.Start {
				return nil, errors.New("invalid or duplicate manifest raw-data object")
			}
			name := file.Path
			if !path.IsAbs(name) && !strings.HasPrefix(name, basePath+"/") {
				name = path.Join(basePath, "_data", name)
			}
			if seen[name] {
				return nil, errors.New("duplicate resolved manifest raw-data object")
			}
			seen[name] = true
			objects = append(objects, ParquetObjectV3{Path: name, Columns: group.Columns, Start: file.Start, End: file.End, Rows: file.End - file.Start})
			if err := numericMetadata(file.Properties, "file_size", "footer_size"); err != nil {
				return nil, err
			}
		}
	}
	for name, stats := range manifest.Stats {
		kind, field, ok := strings.Cut(name, ".")
		if !ok || (kind != "bloom_filter" && kind != "bm25") {
			return nil, errors.New("unclassified manifest statistics")
		}
		if id, err := strconv.ParseInt(field, 10, 64); err != nil || id < 0 || len(stats.Paths) == 0 {
			return nil, errors.New("invalid manifest statistics")
		}
		for _, name := range stats.Paths {
			if !structuralPath(name) {
				return nil, errors.New("invalid manifest statistics path")
			}
		}
		if err := numericMetadata(stats.Metadata, "memory_size"); err != nil {
			return nil, err
		}
	}
	for _, delta := range manifest.DeltaLogs {
		if !structuralPath(delta.Path) || delta.NumEntries < 0 {
			return nil, errors.New("invalid manifest delta log")
		}
	}
	for _, index := range manifest.Indexes {
		if !structuralPath(index.Path) || index.FieldID < 0 || index.NumRows < 0 || index.SerializedSize < 0 || index.MemSize < 0 {
			return nil, errors.New("invalid manifest index metadata")
		}
		if err := numericMetadata(index.Properties); err != nil {
			return nil, err
		}
	}
	// TEXT/LOB encryption has not met the Testable capability requirements.
	if len(manifest.LOBFiles) != 0 {
		return nil, errors.New("non-TEXT campaign unexpectedly produced LOB files")
	}
	return objects, nil
}
