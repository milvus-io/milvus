// Copyright 2026 Zilliz
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

package storagev2

import (
	"encoding/json"
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

const (
	ManifestDir            = "_metadata"
	ManifestFileNamePrefix = "manifest-"
	ManifestFileNameSuffix = ".avro"
)

type ManifestJSON struct {
	ManifestVersion int64  `json:"ver"`
	BasePath        string `json:"base_path"`
	// Size is the byte size of the manifest object of this revision, recorded
	// when the revision was committed, or 0 when unknown. It lets a read skip
	// asking storage for the size and is not part of the revision identity.
	Size int64 `json:"size,omitempty"`
}

func MarshalManifestPath(base string, version int64) string {
	data, _ := json.Marshal(ManifestJSON{ManifestVersion: version, BasePath: base})
	return string(data)
}

// UnmarshalManifestPath leaves error classification to the consuming boundary.
func UnmarshalManifestPath(descriptor string) (string, int64, error) {
	var value ManifestJSON
	if err := json.Unmarshal([]byte(descriptor), &value); err != nil {
		return "", 0, err
	}
	return value.BasePath, value.ManifestVersion, nil
}

// ManifestObjectPath preserves the literal base key, including repeated separators.
func ManifestObjectPath(base string, version int64) string {
	return fmt.Sprintf("%s/%s/%s%d%s", base, ManifestDir, ManifestFileNamePrefix, version, ManifestFileNameSuffix)
}

type ManifestIndexInfo struct {
	ColumnName                string
	IndexName                 string
	IndexType                 string
	Path                      string
	FieldID                   int64
	IndexID                   int64
	BuildID                   int64
	IndexVersion              int64
	NumRows                   int64
	SerializedSize            int64
	MemSize                   int64
	CurrentIndexVersion       int32
	CurrentScalarIndexVersion int32
	IndexStorePathVersion     indexpb.IndexStorePathVersion
	IndexFileKeys             []string
	Properties                map[string]string
}
