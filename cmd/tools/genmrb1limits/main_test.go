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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGeneratedHeaderIsCurrent is the half of this contract a compiler cannot
// carry. C++ resolving `mrb1::kHeaderSize` proves segcore uses whatever the
// checked-in header says; it cannot prove the checked-in header is what the Go
// constants say. Changing a Go constant without regenerating would otherwise
// leave segcore on the old value with every Go test green.
//
// It runs under both Go test targets: `make codecov-go-without-build` ->
// scripts/run_go_codecov.sh, which runs `go test ./...` under cmd/tools, and
// `make test-go` -> scripts/run_go_unittest.sh, which names cmd/tools/...
// directly. The pin it replaces lived in internal/parser, which only the first
// of those covers.
func TestGeneratedHeaderIsCurrent(t *testing.T) {
	root, err := repoRoot()
	require.NoError(t, err)

	// A missing header is a failure, not a reason to skip: this command and
	// internal/core are the same checkout, and skipping would leave the Go and
	// C++ constants unlinked while printing ok.
	onDisk, err := os.ReadFile(filepath.Join(root, outputPath))
	require.NoErrorf(t, err,
		"%s is present but %s is not: if the file moved, update outputPath -- "+
			"silently skipping would leave segcore's limits unlinked from Go's",
		rootSentinel, outputPath)

	require.Equalf(t, string(header()), string(onDisk),
		"%s is stale. A limit changed in pkg/util/roaringfilter without "+
			"regenerating segcore's copy, which is exactly the divergence this "+
			"generator exists to make impossible. Run `make generate-cpp-constants` "+
			"and commit the result.", outputPath)
}

// TestEveryConstantIsRenderable guards the table rather than the file: an entry
// with no type, no name or no value would render a header that does not
// compile, and the C++ build is far away from whoever edits this list.
func TestEveryConstantIsRenderable(t *testing.T) {
	require.NotEmpty(t, constants)
	seen := map[string]struct{}{}
	for _, c := range constants {
		require.NotEmpty(t, c.cppType, "constant %q has no C++ type", c.name)
		require.NotEmpty(t, c.name)
		require.NotEmpty(t, c.value, "constant %q has no value", c.name)
		require.NotEmpty(t, c.comment, "constant %q has no comment", c.name)
		_, duplicate := seen[c.name]
		require.Falsef(t, duplicate, "%s is declared twice", c.name)
		seen[c.name] = struct{}{}
	}
}

// TestStringConstantsAreAsciiPrintable is the one place Go and C++ literals can
// disagree. strconv.Quote is Go quoting: a non-ASCII byte comes out as é,
// which C++ reads as a universal-character-name rather than the byte Go meant,
// and the two sides would then disagree about a magic that both "compile".
// Every value this covers is ASCII today; a magic that is not needs a renderer
// that emits \xNN, not a quiet reinterpretation.
func TestStringConstantsAreAsciiPrintable(t *testing.T) {
	for _, c := range constants {
		if !strings.HasPrefix(c.value, `"`) {
			continue
		}
		for i := 0; i < len(c.value); i++ {
			require.Truef(t, c.value[i] >= 0x20 && c.value[i] < 0x7f,
				"%s renders as %s, which is not plain ASCII: Go and C++ do not "+
					"spell such a literal the same way", c.name, c.value)
		}
	}
}
