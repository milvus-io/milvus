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

package analyzer

import (
	"path/filepath"
	"strings"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// ResolveLocalPaths pins relative TLS paths before schema admission.
// Persist the result so changing operator base directories cannot retarget old schemas.
func ResolveLocalPaths(params, tlsRoot string) (string, error) {
	if params == "" {
		return params, nil
	}
	if !json.Valid([]byte(params)) {
		return "", merr.WrapErrParameterInvalidMsg("invalid analyzer parameters")
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(params))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", merr.WrapErrParameterInvalidMsg("invalid analyzer parameters")
	}
	changed := false
	resolve := func(object map[string]any, key, root string) error {
		name, ok := object[key].(string)
		if !ok || name == "" || filepath.IsAbs(name) {
			return nil
		}
		for _, part := range strings.Split(name, "/") {
			if part == "" || part == "." || part == ".." {
				return merr.WrapErrParameterInvalidMsg("invalid relative analyzer file path")
			}
		}
		if !filepath.IsAbs(root) {
			return merr.WrapErrServiceInternalMsg("analyzer file base directory must be absolute")
		}
		object[key] = filepath.Join(root, name)
		changed = true
		return nil
	}
	var visit func(any) error
	visit = func(value any) error {
		switch object := value.(type) {
		case map[string]any:
			for _, child := range object {
				if err := visit(child); err != nil {
					return err
				}
			}
			if object["type"] == "grpc" {
				if tls, ok := object["tls"].(map[string]any); ok {
					for _, key := range []string{"ca_cert", "client_cert", "client_key"} {
						if err := resolve(tls, key, tlsRoot); err != nil {
							return err
						}
					}
				}
			}
		case []any:
			for _, child := range object {
				if err := visit(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(value); err != nil {
		return "", err
	}
	if !changed {
		return params, nil
	}
	resolved, err := json.Marshal(value)
	if err != nil {
		return "", merr.WrapErrServiceInternalMsg("cannot serialize analyzer parameters")
	}
	return string(resolved), nil
}
