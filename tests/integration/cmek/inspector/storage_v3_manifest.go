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
	"path"
	"strconv"
	"strings"

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
	if err := json.Unmarshal([]byte(raw), &locator); err != nil {
		return locator, err
	}
	if locator.BasePath == "" || locator.Version <= 0 {
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

// Only fields needed to locate the current raw-data objects are decoded.
type manifestV3 struct {
	ColumnGroups []manifestV3ColumnGroup `json:"column_groups"`
}

type manifestV3ColumnGroup struct {
	Columns []string         `json:"columns"`
	Files   []manifestV3File `json:"files"`
}

type manifestV3File struct {
	Path  string `json:"path"`
	Start int64  `json:"start_index"`
	End   int64  `json:"end_index"`
}

type ParquetObjectV3 struct {
	Path    string
	Columns []string
	Start   int64
	End     int64
	Rows    int64
}

// ParseParquetObjectsV3 decodes the object references in an exact manifest,
// returning every physical raw-data object resolved against its base path.
func ParseParquetObjectsV3(raw []byte, basePath string) ([]ParquetObjectV3, error) {
	if basePath == "" {
		return nil, errors.New("invalid manifest base path")
	}
	decoder, err := ocf.NewDecoder(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
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
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		return nil, err
	}
	return manifest.parquetObjects(basePath)
}

func (manifest *manifestV3) parquetObjects(basePath string) ([]ParquetObjectV3, error) {
	if len(manifest.ColumnGroups) == 0 {
		return nil, errors.New("manifest has no column groups")
	}
	var objects []ParquetObjectV3
	seen := make(map[string]bool)
	columns := make(map[string]bool)
	for _, group := range manifest.ColumnGroups {
		if len(group.Columns) == 0 || len(group.Files) == 0 {
			return nil, errors.New("manifest has empty column group")
		}
		for _, column := range group.Columns {
			if id, err := strconv.ParseInt(column, 10, 64); err != nil || id < 0 || columns[column] {
				return nil, errors.New("invalid or duplicate manifest column")
			}
			columns[column] = true
		}
		for _, file := range group.Files {
			if file.Path == "" || file.Start < 0 || file.End <= file.Start {
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
		}
	}
	return objects, nil
}
