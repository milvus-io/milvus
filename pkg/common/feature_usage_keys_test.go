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

package common

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsOfficialFeatureKey(t *testing.T) {
	assert.True(t, IsOfficialFeatureKey(MmapEnabledKey))
	assert.True(t, IsOfficialFeatureKey(CollectionTTLConfigKey))
	assert.True(t, IsOfficialFeatureKey(DatabaseForceDenyWritingKey))
	assert.True(t, IsOfficialFeatureKey("efConstruction"))
	assert.False(t, IsOfficialFeatureKey("my.custom.key"))
	assert.False(t, IsOfficialFeatureKey(""))
	assert.False(t, IsOfficialFeatureKey("MMAP.ENABLED"), "keys are case-sensitive")
}

// Every dotted lower-case string constant in common.go is a property key that
// users may set, and the report names it. This test fails when a new dotted
// key constant is added to common.go without being added to the allowlist, so
// the allowlist cannot drift from the constants file.
func TestOfficialFeatureKeysCoverDottedConstants(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "common.go", nil, 0)
	require.NoError(t, err)

	dotted := regexp.MustCompile(`^[a-z][a-zA-Z0-9_]*(\.[a-zA-Z0-9_]+)+$`)
	// Constants that look like keys but are not property keys.
	exempt := map[string]struct{}{
		"uber-trace-id":           {},
		"client-request-unixmsec": {},
	}

	var missing []string
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, v := range vs.Values {
				lit, ok := v.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				val, err := strconv.Unquote(lit.Value)
				if err != nil {
					continue
				}
				if _, skip := exempt[val]; skip || !dotted.MatchString(val) {
					continue
				}
				if !IsOfficialFeatureKey(val) {
					missing = append(missing, vs.Names[i].Name+"="+val)
				}
			}
		}
	}
	assert.Empty(t, missing, "dotted key constants in common.go missing from officialFeatureKeys")
}
