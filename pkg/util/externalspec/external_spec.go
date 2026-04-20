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

package externalspec

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// ExternalSpec represents the parsed external collection specification
type ExternalSpec struct {
	Format  string            `json:"format"`          // e.g., "parquet", "vortex", "lance-table"
	Columns []string          `json:"columns"`         // optional: specific columns to load
	Extfs   map[string]string `json:"extfs,omitempty"` // optional: extfs config overrides (non-sensitive only)
}

// supportedFormats lists the file formats supported for external collections
var supportedFormats = map[string]bool{
	"parquet":     true,
	"lance-table": true,
	"vortex":      true,
}

// allowedExtfsKeys lists extfs configuration keys that can be passed via ExternalSpec.
// Credential keys are allowed because cross-bucket scenarios require different
// authentication from the main Milvus storage (e.g., local MinIO vs external AWS S3).
// Note: these values are stored in etcd as part of CollectionSchema.
var allowedExtfsKeys = map[string]bool{
	"use_iam":          true,
	"use_ssl":          true,
	"use_virtual_host": true,
	"region":           true,
	"cloud_provider":   true,
	"iam_endpoint":     true,
	"storage_type":     true,
	"ssl_ca_cert":      true,
	"access_key_id":    true,
	"access_key_value": true,
}

// booleanExtfsKeys lists extfs keys that only accept "true" or "false".
var booleanExtfsKeys = map[string]bool{
	"use_iam":          true,
	"use_ssl":          true,
	"use_virtual_host": true,
}

// ParseExternalSpec parses the JSON external spec string
func ParseExternalSpec(specStr string) (*ExternalSpec, error) {
	if specStr == "" {
		return &ExternalSpec{Format: "parquet"}, nil // default
	}

	var spec ExternalSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid external spec JSON: %s", err.Error())
	}

	if spec.Format == "" {
		spec.Format = "parquet" // default format
	}

	if !supportedFormats[spec.Format] {
		return nil, merr.WrapErrParameterInvalidMsg("unsupported format %q, supported formats: %s",
			spec.Format, strings.Join(sortedKeys(supportedFormats), ", "))
	}

	for key, val := range spec.Extfs {
		if !allowedExtfsKeys[key] {
			return nil, merr.WrapErrParameterInvalidMsg("extfs key %q is not allowed; allowed keys: %s",
				key, strings.Join(sortedKeys(allowedExtfsKeys), ", "))
		}
		if booleanExtfsKeys[key] && val != "true" && val != "false" {
			return nil, merr.WrapErrParameterInvalidMsg("extfs key %q must be \"true\" or \"false\", got %q", key, val)
		}
	}

	return &spec, nil
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
