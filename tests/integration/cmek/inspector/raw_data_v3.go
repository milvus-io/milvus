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
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/cockroachdb/errors"

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
	Identity     string
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
		references = append(references, ManifestReferenceV3{collectionID, segment.GetID(), segment.GetNumOfRows(), segment.GetManifestPath(), locator})
	}
	return references, nil
}
