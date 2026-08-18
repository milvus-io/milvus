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

package planparserv2

// A wire-format limit that segcore enforces and Go enforces is one contract
// with two compilers. Nothing in the build links them, so a value edited on one
// side alone compiles and passes CI; the symptom shows up in production as the
// proxy admitting a blob every querynode refuses, or the SDK refusing to build
// one the cluster would have taken.
//
// This file is the shared half of pinning such constants. A format supplies a
// table (see roaring_segcore_parity_test.go) and calls
// assertCppConstantParity; the intent is that the next membership format --
// client/sbbf's MBF1 has the same three-way contract and no pin -- adds a table
// here rather than a second scanner.
//
// It lives in this package because the plan parser is the only Go code that
// sits on both sides: it is in segcore's module, so reading internal/core
// crosses no boundary, and it already owns the Go-to-Go pin between the SDK
// builder and the proxy validator.
//
// # How it checks, and what that buys
//
// The expected declaration is *built from the Go constant* and compared as
// text. There is no C++ expression evaluator: an evaluator has to model
// operator precedence, brace initializers and -- the case that actually bites
// -- implicit narrowing, where `uint16_t kX = uint64_t{1} << 18` really is 0.
// Modelling those is where such a checker goes quietly wrong, and a checker
// that is quietly wrong is worse than none. Comparing text cannot: a changed
// value, a changed declared type and a changed name are all the same plain
// mismatch, and neither side can be edited without the other.
//
// The cost is that pinned constants must be declared as literals, in the shape
// `constexpr <type> <name> = <literal>;`. That is a real constraint on the C++,
// and it is the trade: nine numbers being greppable in both languages beats
// `128 * 1024 * 1024` in the declaration, and the readable form still fits in a
// trailing comment. An `static` prefix is accepted (class members carry it,
// namespace-scope constants do not) and whitespace is normalised first, so the
// line wraps .clang-format's ColumnLimit forces are not a divergence.
//
// # What it does not cover
//
// Declarations only, never use sites. Changing `/ kMinEntryBytes` to `/ 16`, or
// a `>=` to a `>`, diverges the two sides exactly as a changed constant would,
// and passes. That is the widest gap: the pin says the two sides agree on what
// the numbers are, not on what they are used for. Generating the C++ constants
// from the Go ones, or static_asserting them against a generated header, would
// close it; both are heavier than this, which has no build dependency and runs
// in the same `go test` invocation that already gates the Go halves.
//
// # Where it runs
//
// The GitHub Actions "UT for Go" job, via `make codecov-go-without-build` ->
// scripts/run_go_codecov.sh, which runs `go test ./...` under internal/. Note
// that scripts/run_go_unittest.sh -- `make test-go` -- does not cover
// internal/parser, so this does not run locally under that target.

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cppConstantPin is one C++ constant declaration pinned to a Go value.
type cppConstantPin struct {
	name string
	// decl is the declaration the C++ source must carry, whitespace-normalised
	// and without any `static` prefix. Always produced by pinCppInt or
	// pinCppString from the Go constant, never written out by hand -- a
	// hand-written expectation could be updated alongside the C++ and leave the
	// Go side behind, which is the divergence this exists to catch.
	decl string
}

func pinCppInt(cppType, name string, value uint64) cppConstantPin {
	return cppConstantPin{
		name: name,
		decl: fmt.Sprintf("constexpr %s %s = %d;", cppType, name, value),
	}
}

func pinCppString(name, value string) cppConstantPin {
	return cppConstantPin{
		name: name,
		// %q is C++ source syntax for the literals these constants use. A value
		// needing an escape C++ spells differently would show up as a mismatch
		// against the real declaration rather than passing wrongly.
		decl: fmt.Sprintf("constexpr std::string_view %s = %q;", name, value),
	}
}

// cppConstantName matches any k-prefixed constexpr identifier whatever its type
// spelling. The completeness check must see declarations in forms the pinned
// comparison would not match -- those are exactly the ones that would otherwise
// slip in unnoticed.
var cppConstantName = regexp.MustCompile(`constexpr\s[^;=]*?\b(k[A-Z]\w*)\s*[={]`)

// One alternation, left to right, so neither comment kind can eat the other's
// terminator: block-first lets a `//` inside a block swallow its `*/`, and
// line-first lets a `//` mentioning /* open a block. Whichever opens earliest
// wins, as in C++.
var cppComment = regexp.MustCompile(`(?s)/\*.*?\*/|//[^\n]*`)

var (
	cppWhitespace = regexp.MustCompile(`\s+`)
	// A pinned declaration holds exactly one `=`, its initializer's, because
	// the initializer is a literal. Applied only to a matched declaration, so
	// a `>=` elsewhere in the source is never reached.
	cppAssign = regexp.MustCompile(`\s*=\s*`)
)

// assertCppConstantParity checks two things against the given C++ sources,
// which are read as text relative to the repository root:
//
//   - every pinned constant is declared exactly once, with exactly the
//     declaration its Go counterpart implies;
//   - every k-prefixed constexpr the sources declare is either pinned or named
//     in unpinned. This is the direction that keeps the check from eroding: a
//     constant added to the C++ is invisible until someone classifies it.
//
// Every failure is reported, rather than stopping at the first, so one run
// tells the reader everything that diverged.
func assertCppConstantParity(t *testing.T, sources []string, pinned []cppConstantPin, unpinned []string) {
	t.Helper()
	require.NotEmpty(t, sources, "no C++ sources to check")
	require.NotEmpty(t, pinned, "no constants pinned; this check would assert nothing")

	root := findCppSourceRoot(t)
	bodies := make(map[string]string, len(sources))
	for _, source := range sources {
		bodies[source] = normalizeCppWhitespace(stripCppComments(readCppSource(t, root, source)))
	}

	for _, pin := range pinned {
		declarations := cppDeclarationsOf(bodies, pin.name)
		switch {
		case len(declarations) == 0:
			// Renaming a constant lands here, and separately trips the
			// completeness check below under its new name. Say which repair is
			// wanted; conflating this with a value change sends the reader
			// looking for a divergence that never happened.
			assert.Failf(t, "constant not declared",
				"%v no longer declare %s in a form this check can see. Either the "+
					"constant was renamed or removed -- keep the name, or update the pin "+
					"table -- or it was rewritten away from the "+
					"`constexpr <type> <name> = <literal>;` form this contract is pinned "+
					"in.", sources, pin.name)
		case len(declarations) > 1:
			// Not "the last match wins": a name declared twice, whether by an
			// #ifdef pair or a decoy left at another scope, means this check
			// cannot tell which one the compiler takes, so it would grade
			// against a declaration that is not the live one.
			assert.Failf(t, "constant declared more than once",
				"%s is declared %d times across %v, so this check cannot tell which one "+
					"the compiler takes:\n%s",
				pin.name, len(declarations), sources, strings.Join(declarations, "\n"))
		default:
			// TrimPrefix, not a regex alternation, so the reported "want" is
			// the text as declared: `static` is a scope detail this pin does
			// not police.
			assert.Equalf(t, pin.decl,
				strings.TrimPrefix(canonicalCppDeclaration(declarations[0]), "static "),
				"segcore's declaration of %s does not match the Go constant.\n"+
					"The expected text is built from the Go value, so this means either the "+
					"two sides were changed independently, or the declaration was rewritten "+
					"away from the literal `constexpr <type> <name> = <literal>;` form this "+
					"contract is pinned in. Fix the side that is wrong; do not relax this "+
					"check.", pin.name)
		}
	}

	known := slices.Clone(unpinned)
	for _, pin := range pinned {
		known = append(known, pin.name)
	}
	for _, name := range declaredCppConstants(bodies) {
		assert.Containsf(t, known, name,
			"%s is declared in %v and nothing classifies it; pin it against its Go "+
				"counterpart, or list it as unpinned with the reason it needs none",
			name, sources)
	}
}

// cppDeclarationsOf returns every constexpr declaration of name across the
// supplied normalised bodies, in source order.
func cppDeclarationsOf(bodies map[string]string, name string) []string {
	// `[^;]` cannot cross a statement boundary, so a match is one declaration
	// and a use site -- which carries no constexpr of its own -- cannot produce
	// one.
	declaration := regexp.MustCompile(
		`(?:static\s+)?constexpr\s[^;]*?\b` + regexp.QuoteMeta(name) + `\b[^;]*;`)
	var out []string
	for _, source := range slices.Sorted(maps.Keys(bodies)) {
		out = append(out, declaration.FindAllString(bodies[source], -1)...)
	}
	return out
}

func declaredCppConstants(bodies map[string]string) []string {
	seen := map[string]struct{}{}
	for _, body := range bodies {
		for _, m := range cppConstantName.FindAllStringSubmatch(body, -1) {
			seen[m[1]] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// stripCppComments removes comments so a commented-out declaration cannot be
// mistaken for a live one, and so a trailing comment cannot end up inside a
// compared declaration. It does not model string literals containing comment
// markers, which these sources do not have.
func stripCppComments(body string) string {
	return cppComment.ReplaceAllString(body, " ")
}

// normalizeCppWhitespace collapses runs of whitespace to one space, so the line
// breaks .clang-format's ColumnLimit forces between a type, a name and its
// initializer do not read as a divergence.
func normalizeCppWhitespace(body string) string {
	return cppWhitespace.ReplaceAllString(body, " ")
}

// canonicalCppDeclaration puts one already-normalised declaration into the
// spelling pinCppInt and pinCppString produce, so that spacing the compiler
// ignores is not reported as a contract divergence. .clang-format would settle
// this spacing anyway, but a Go test failing over a missing space around `=`
// sends the reader hunting in the wrong language.
func canonicalCppDeclaration(decl string) string {
	decl = cppAssign.ReplaceAllString(normalizeCppWhitespace(strings.TrimSpace(decl)), " = ")
	return strings.ReplaceAll(decl, " ;", ";")
}

// cppSourceRootSentinel proves the C++ tree is in the checkout. It must be
// something no reorganisation of an individual source would remove.
const cppSourceRootSentinel = "internal/core/CMakeLists.txt"

// readCppSource reads a C++ source. This package and internal/core are the same
// module in the same checkout, so a source this check cannot find is a failure,
// not a reason to skip: an earlier revision of this pin skipped when it could
// not locate its sources and printed ok, which is the failure mode the pin
// exists to remove.
func readCppSource(t *testing.T, root, source string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, source))
	require.NoErrorf(t, err,
		"%s is present but %s is not: if the file moved or was renamed, update the "+
			"source list -- silently skipping would leave the Go and C++ constants "+
			"unpinned", cppSourceRootSentinel, source)
	return string(body)
}

// findCppSourceRoot walks up looking for cppSourceRootSentinel and returns the
// directory holding it. Walking beats resolving a fixed "../../.." because the
// latter ties the pin to this package's depth, so moving the package would
// break it.
func findCppSourceRoot(t *testing.T) string {
	t.Helper()
	start, err := filepath.Abs(".")
	require.NoError(t, err)
	for dir := start; ; {
		if _, err := os.Stat(filepath.Join(dir, cppSourceRootSentinel)); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqualf(t, dir, parent,
			"%s is nowhere above %s, but internal/core and this package are the same "+
				"module, so it has to be", cppSourceRootSentinel, start)
		dir = parent
	}
}
