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

package rootcoord

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collID2MetaWriters are the only functions allowed to write collID2Meta
// entries. Everything else must call them.
var collID2MetaWriters = map[string]struct{}{
	"setCollectionLocked":    {},
	"deleteCollectionLocked": {},
}

// isCollID2MetaEntry reports whether expr is `<x>.collID2Meta[<k>]`.
func isCollID2MetaEntry(expr ast.Expr) bool {
	idx, ok := expr.(*ast.IndexExpr)
	if !ok {
		return false
	}
	sel, ok := idx.X.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "collID2Meta"
}

// writesCollID2MetaEntry reports whether assigning to lhs writes a
// collID2Meta entry or a field reached through one, e.g.
// `mt.collID2Meta[id] = c` or `mt.collID2Meta[id].Partitions = p`.
func writesCollID2MetaEntry(lhs ast.Expr) bool {
	for {
		if isCollID2MetaEntry(lhs) {
			return true
		}
		switch e := lhs.(type) {
		case *ast.SelectorExpr:
			lhs = e.X
		case *ast.IndexExpr:
			lhs = e.X
		case *ast.StarExpr:
			lhs = e.X
		case *ast.ParenExpr:
			lhs = e.X
		default:
			return false
		}
	}
}

// findCollID2MetaWrites returns the positions of collID2Meta entry writes
// outside collID2MetaWriters.
func findCollID2MetaWrites(fset *token.FileSet, file *ast.File) []string {
	var found []string
	report := func(n ast.Node) {
		found = append(found, fset.Position(n.Pos()).String())
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if _, allowed := collID2MetaWriters[fn.Name.Name]; allowed {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch s := n.(type) {
			case *ast.AssignStmt:
				for _, lhs := range s.Lhs {
					if writesCollID2MetaEntry(lhs) {
						report(s)
					}
				}
			case *ast.IncDecStmt:
				if writesCollID2MetaEntry(s.X) {
					report(s)
				}
			case *ast.CallExpr:
				if ident, ok := s.Fun.(*ast.Ident); ok && ident.Name == "delete" && len(s.Args) > 0 {
					if sel, ok := s.Args[0].(*ast.SelectorExpr); ok && sel.Sel.Name == "collID2Meta" {
						report(s)
					}
				}
			}
			return true
		})
	}
	return found
}

func TestFindCollID2MetaWrites(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		found int
	}{
		{"entry assign", "mt.collID2Meta[1] = c", 1},
		{"field through entry", "mt.collID2Meta[1].Partitions = p", 1},
		{"nested field through entry", "mt.collID2Meta[1].Schema.Name = n", 1},
		{"incdec through entry", "mt.collID2Meta[1].ShardsNum++", 1},
		{"delete entry", "delete(mt.collID2Meta, 1)", 1},
		{"reset map", "mt.collID2Meta = make(map[int64]*model.Collection)", 0},
		{"read entry", "c := mt.collID2Meta[1]; _ = c", 0},
		{"write local copy", "c := mt.collID2Meta[1]; c.Partitions = p", 0},
		{"other map", "mt.partitionName2ID[1] = m", 0},
		{"delete other map", "delete(mt.partitionName2ID, 1)", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := fmt.Sprintf("package rootcoord\nfunc f() {\n%s\n}\n", tc.body)
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "case.go", src, 0)
			require.NoError(t, err)
			assert.Len(t, findCollID2MetaWrites(fset, file), tc.found)
		})
	}

	t.Run("allowed writer", func(t *testing.T) {
		src := "package rootcoord\nfunc setCollectionLocked() { mt.collID2Meta[1] = c }\n"
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "case.go", src, 0)
		require.NoError(t, err)
		assert.Empty(t, findCollID2MetaWrites(fset, file))
	})
}

// TestCollID2MetaWritesGoThroughHelpers keeps every collID2Meta mutation in
// setCollectionLocked / deleteCollectionLocked.
func TestCollID2MetaWritesGoThroughHelpers(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	var found []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		found = append(found, findCollID2MetaWrites(fset, file)...)
	}
	assert.Empty(t, found, "collID2Meta must only be written by setCollectionLocked / deleteCollectionLocked")
}
