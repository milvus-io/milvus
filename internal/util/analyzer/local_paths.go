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

// ResolveLocalPaths pins relative dictionary and TLS paths before schema admission.
// Persist the result so changing operator base directories cannot retarget old schemas.
func ResolveLocalPaths(params, dictionaryRoot, tlsRoot string) (string, error) {
	var visit func(json.RawMessage) (json.RawMessage, bool, error)
	resolve := func(object map[string]json.RawMessage, key, root string) (bool, error) {
		var name string
		value, ok := object[key]
		if !ok || json.Unmarshal(value, &name) != nil || name == "" || filepath.IsAbs(name) {
			return false, nil
		}
		for _, part := range strings.Split(name, "/") {
			if part == "" || part == "." || part == ".." {
				return false, merr.WrapErrParameterInvalidMsg("invalid relative analyzer file path")
			}
		}
		if !filepath.IsAbs(root) {
			return false, merr.WrapErrServiceInternalMsg("analyzer file base directory must be absolute")
		}
		object[key], _ = json.Marshal(filepath.Join(root, name))
		return true, nil
	}
	visit = func(raw json.RawMessage) (json.RawMessage, bool, error) {
		trimmed := strings.TrimSpace(string(raw))
		changed := false
		switch {
		case strings.HasPrefix(trimmed, "{"):
			var object map[string]json.RawMessage
			if err := json.Unmarshal(raw, &object); err != nil {
				return nil, false, merr.WrapErrParameterInvalidMsg("invalid analyzer parameters")
			}
			for key, value := range object {
				next, updated, err := visit(value)
				if err != nil {
					return nil, false, err
				}
				object[key] = next
				changed = changed || updated
			}
			var kind string
			_ = json.Unmarshal(object["type"], &kind)
			if kind == "local" {
				updated, err := resolve(object, "path", dictionaryRoot)
				if err != nil {
					return nil, false, err
				}
				changed = changed || updated
			}
			if kind == "grpc" {
				var tls map[string]json.RawMessage
				if json.Unmarshal(object["tls"], &tls) == nil && tls != nil {
					for _, key := range []string{"ca_cert", "client_cert", "client_key"} {
						updated, err := resolve(tls, key, tlsRoot)
						if err != nil {
							return nil, false, err
						}
						changed = changed || updated
					}
					object["tls"], _ = json.Marshal(tls)
				}
			}
			if changed {
				raw, _ = json.Marshal(object)
			}
		case strings.HasPrefix(trimmed, "["):
			var array []json.RawMessage
			if err := json.Unmarshal(raw, &array); err != nil {
				return nil, false, merr.WrapErrParameterInvalidMsg("invalid analyzer parameters")
			}
			for i, value := range array {
				next, updated, err := visit(value)
				if err != nil {
					return nil, false, err
				}
				array[i] = next
				changed = changed || updated
			}
			if changed {
				raw, _ = json.Marshal(array)
			}
		}
		return raw, changed, nil
	}
	if params == "" {
		return params, nil
	}
	if !json.Valid([]byte(params)) {
		return "", merr.WrapErrParameterInvalidMsg("invalid analyzer parameters")
	}
	resolved, _, err := visit(json.RawMessage(params))
	return string(resolved), err
}
