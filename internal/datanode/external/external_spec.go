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

package external

import (
	"encoding/json"
	"fmt"
)

// ExternalSpec represents the parsed external collection specification
type ExternalSpec struct {
	Format  string   `json:"format"`  // e.g., "parquet", "csv"
	Columns []string `json:"columns"` // optional: specific columns to load
}

// supportedFormats lists the file formats supported for external collections
var supportedFormats = map[string]bool{
	"parquet": true,
	"csv":     true,
	"json":    true,
}

// ParseExternalSpec parses the JSON external spec string
func ParseExternalSpec(specStr string) (*ExternalSpec, error) {
	if specStr == "" {
		return &ExternalSpec{Format: "parquet"}, nil // default
	}

	var spec ExternalSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return nil, fmt.Errorf("invalid external spec JSON: %w", err)
	}

	if spec.Format == "" {
		spec.Format = "parquet" // default format
	}

	if !supportedFormats[spec.Format] {
		return nil, fmt.Errorf("unsupported format %q, supported formats: parquet, csv, json", spec.Format)
	}

	return &spec, nil
}
