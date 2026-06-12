package catalog

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogPackageImportBoundary(t *testing.T) {
	const (
		internalPrefix = "github.com/milvus-io/milvus/internal/"
		internalProto  = "github.com/milvus-io/milvus/pkg/v3/proto/"
	)
	bannedProtoPackages := map[string]struct{}{
		"datapb":      {},
		"indexpb":     {},
		"querypb":     {},
		"streamingpb": {},
	}
	bannedFragments := []string{
		"/etcd",
		"/etcd/",
		"/kv",
		"/kv/",
	}
	bannedPublicTerms := []string{
		"etcd",
		"kv",
		"tikv",
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	sourceFiles := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		sourceFiles++

		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", name), nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse imports from %s: %v", name, err)
		}
		for _, imported := range file.Imports {
			path := strings.Trim(imported.Path.Value, `"`)
			if strings.HasPrefix(path, internalPrefix) {
				t.Fatalf("%s imports internal package %q", name, path)
			}
			if strings.HasPrefix(path, internalProto) {
				pkg := strings.TrimPrefix(path, internalProto)
				if _, banned := bannedProtoPackages[pkg]; banned {
					t.Fatalf("%s imports internal proto package %q", name, path)
				}
			}
			for _, fragment := range bannedFragments {
				if strings.Contains(path, fragment) {
					t.Fatalf("%s imports storage/backend package %q", name, path)
				}
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.Ident:
				failIfBackendTerm(t, name, n.Name, bannedPublicTerms)
			case *ast.BasicLit:
				failIfBackendTerm(t, name, n.Value, bannedPublicTerms)
			}
			return true
		})
	}
	if sourceFiles == 0 {
		t.Fatal("expected catalog package source files")
	}
}

func failIfBackendTerm(t *testing.T, filename string, value string, bannedTerms []string) {
	t.Helper()
	lowered := strings.ToLower(value)
	for _, term := range bannedTerms {
		if strings.Contains(lowered, term) {
			t.Fatalf("%s exposes storage/backend term %q in %q", filename, term, value)
		}
	}
}
