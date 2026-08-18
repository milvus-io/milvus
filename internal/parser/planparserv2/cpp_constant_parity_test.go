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
// This file is the format-agnostic half of pinning such constants. A format
// supplies a table (see roaring_segcore_parity_test.go) and calls
// assertCppConstantParity; the intent is that the next membership format --
// client/sbbf's MBF1 has the same three-way contract and no pin -- adds a table
// rather than a second scanner.
//
// It lives in this package because the plan parser is the only Go code sitting
// on both sides: it is in segcore's module, so reading internal/core crosses no
// boundary, and it already owns the Go-to-Go pin between the SDK builder and
// the proxy validator.
//
// # How a constant is checked
//
// Each pin names a declared type, a name and a Go value, and the check splits
// the C++ declaration at its `=`:
//
//   - the declarator -- `constexpr <type> <name> =` -- is compared as text
//     against the pin. Pinning the type by text is what makes the initializer
//     safe to evaluate: `uint16_t kX = uint64_t{1} << 18` really is 0, and the
//     only way to hide that from a value comparison is to change the declared
//     type, which is a text mismatch here;
//   - the initializer is evaluated and compared to the Go value, and the result
//     is range-checked against the declared type, so narrowing fails even if
//     someone updates both the C++ type and this table.
//
// The evaluator handles only what these declarations use: literals with the
// usual suffixes and digit separators, `*`, `<<`, parentheses, and the
// `uint64_t{1}` brace-init form. Anything else is an error, not a guess. Note
// which way its own bugs point: a mis-evaluated expression yields a value the
// Go constant does not have, so it fails loudly. It cannot pass wrongly unless
// it agrees with the compiler, which is the whole job.
//
// Evaluating rather than demanding a literal is deliberate. The alternative --
// pinning the initializer as text -- forces segcore to spell its limits as
// magic decimals so that a Go test can match them, which is a permanent cost in
// production C++ to save complexity in a test. `128 * 1024 * 1024` says what it
// means.
//
// Spelling that the compiler ignores is normalised away before comparing:
// whitespace and the line breaks .clang-format's ColumnLimit forces, `static`
// and `inline`, a `std::` qualifier, and digit separators. A Go test failing
// over `std::size_t` or `134'217'728` would send the reader hunting in the
// wrong language.
//
// # What it does not cover
//
// Declarations only, never use sites. Changing `/ kMinEntryBytes` to `/ 16`, or
// a `>=` to a `>`, diverges the two sides exactly as a changed constant would,
// and passes. That is the widest gap: the pin says the two sides agree on what
// the numbers are, not on what they are used for.
//
// It also only runs C++ -> classified. Nothing walks the Go constants and
// checks each has a pin, so adding one to the Go side and forgetting the table
// is invisible. Generating the C++ constants from the Go ones, or
// static_asserting them against a generated header, would close both; that is
// the upgrade path if this ever costs someone real time.
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
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cppConstantPin is one C++ constant declaration pinned to a Go value.
type cppConstantPin struct {
	cppType string
	name    string
	// want is what the initializer must denote: a uint64 for an integer
	// constant, a string for a std::string_view one.
	want any
}

func pinCppInt(cppType, name string, value uint64) cppConstantPin {
	return cppConstantPin{cppType: cppType, name: name, want: value}
}

func pinCppString(name, value string) cppConstantPin {
	return cppConstantPin{cppType: "std::string_view", name: name, want: value}
}

// declarator is the text the C++ must carry up to and including its `=`, in the
// normalised spelling canonicalCppDeclaration produces.
func (p cppConstantPin) declarator() string {
	return fmt.Sprintf("constexpr %s %s =", stripCppStd(p.cppType), p.name)
}

var (
	// cppConstantName matches a k-prefixed constant declaration whatever its
	// type spelling, and `const` as well as `constexpr`: an integral
	// `static const uint64_t kFoo = 7;` behaves identically and would otherwise
	// be invisible to the completeness check. `[^;=]` keeps it from reaching
	// past an initializer into a use site.
	cppConstantName = regexp.MustCompile(`const(?:expr)?\s[^;=]*?\b(k[A-Z]\w*)\s*[={]`)

	// One alternation, left to right, so neither comment kind can eat the
	// other's terminator: block-first lets a `//` inside a block swallow its
	// `*/`, and line-first lets a `//` mentioning /* open a block. Whichever
	// opens earliest wins, as in C++.
	cppComment = regexp.MustCompile(`(?s)/\*.*?\*/|//[^\n]*`)

	cppWhitespace = regexp.MustCompile(`\s+`)
	// A declaration holds exactly one `=`, its initializer's; these constants
	// have no comparison in their initializers. Applied only to an already
	// matched declaration, so a `>=` elsewhere in the source is never reached.
	cppAssign = regexp.MustCompile(`\s*=\s*`)
	// `static` and `inline` are storage and linkage, not part of the contract,
	// and `constexpr static` is as valid as `static constexpr`.
	cppSpecifier = regexp.MustCompile(`\b(?:static|inline)\s+`)

	cppBraceInit    = regexp.MustCompile(`\b\w+\s*\{\s*([^{}]*?)\s*\}`)
	cppInnerParens  = regexp.MustCompile(`\(([^()]*)\)`)
	cppIntegerToken = regexp.MustCompile(`^(?:0[xX][0-9a-fA-F']+|[0-9][0-9']*)[uUlL]*$`)
	cppStringLit    = regexp.MustCompile(`"([^"]*)"`)
)

// cppIntWidths lists the declared types a pin may use. An unlisted type is an
// error rather than an assumption: assuming 64 bits would silently accept a
// narrowing, and assuming anything narrower would invent one.
var cppIntWidths = map[string]uint{
	"uint16_t": 16,
	"uint32_t": 32,
	"uint64_t": 64,
	// size_t is 64-bit on every platform milvus builds for.
	"size_t": 64,
}

// assertCppConstantParity checks two things against the given C++ sources,
// which are read as text relative to the repository root:
//
//   - every pinned constant is declared exactly once, with the declared type
//     the pin names and an initializer denoting the pin's Go value;
//   - every k-prefixed constant the sources declare is either pinned or named
//     in unpinned. This is the direction that keeps the check from eroding: a
//     constant added to the C++ is invisible until someone classifies it.
//
// Every failure is reported rather than stopping at the first, so one run tells
// the reader everything that diverged.
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
		if declaration, ok := soleCppDeclaration(t, bodies, sources, pin.name); ok {
			assertCppDeclarationMatches(t, pin, declaration)
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

// soleCppDeclaration reports the one declaration of name, or fails describing
// which repair is wanted. Conflating "renamed" with "declared twice" sends the
// reader looking for a problem that does not exist.
func soleCppDeclaration(t *testing.T, bodies map[string]string, sources []string, name string) (string, bool) {
	t.Helper()
	declarations := cppDeclarationsOf(bodies, name)
	switch {
	case len(declarations) == 0:
		// Renaming a constant lands here, and separately trips the completeness
		// check under its new name.
		assert.Failf(t, "constant not declared",
			"%v no longer declare %s in a form this check can see. Either the constant "+
				"was renamed or removed -- keep the name, or update the pin table -- or "+
				"it was rewritten away from the `constexpr <type> <name> = <initializer>;` "+
				"shape this contract is pinned in.", sources, name)
		return "", false
	case len(declarations) > 1:
		// Not "the last match wins": a name declared twice, whether by an
		// #ifdef pair or a decoy left at another scope, means this check cannot
		// tell which one the compiler takes, so it would grade against a
		// declaration that is not the live one.
		assert.Failf(t, "constant declared more than once",
			"%s is declared %d times across %v, so this check cannot tell which one the "+
				"compiler takes:\n%s",
			name, len(declarations), sources, strings.Join(declarations, "\n"))
		return "", false
	}
	return declarations[0], true
}

func assertCppDeclarationMatches(t *testing.T, pin cppConstantPin, declaration string) {
	t.Helper()
	declarator, initializer, ok := strings.Cut(canonicalCppDeclaration(declaration), "= ")
	if !assert.Truef(t, ok, "cannot split %s's declaration at its initializer: %s",
		pin.name, declaration) {
		return
	}
	declarator += "="
	initializer = strings.TrimSuffix(strings.TrimSpace(initializer), ";")

	// The declared type is half the contract, and the half that makes the
	// initializer safe to evaluate.
	if !assert.Equalf(t, pin.declarator(), declarator,
		"segcore declares %s with a different type than the pin expects.\n"+
			"Full declaration: %s", pin.name, declaration) {
		return
	}

	switch want := pin.want.(type) {
	case string:
		// C++ concatenates adjacent string literals, so `"MRB1" "X"` is a
		// five-byte view. Reading only the first would report MRB1 and pass
		// while every querynode rejected every blob the SDK builds.
		got := ""
		for _, lit := range cppStringLit.FindAllStringSubmatch(initializer, -1) {
			got += lit[1]
		}
		assert.Equalf(t, want, got,
			"%s diverged between Go and segcore (initializer %q)", pin.name, initializer)
	case uint64:
		got, err := evalCppIntExpr(initializer)
		if !assert.NoErrorf(t, err,
			"cannot evaluate segcore's initializer for %s: %s\n"+
				"The value may well be correct -- teach evalCppIntExpr the new form "+
				"rather than assuming a divergence.", pin.name, declaration) {
			return
		}
		width, known := cppIntWidths[stripCppStd(pin.cppType)]
		if !assert.Truef(t, known,
			"%s is declared %s, which this check cannot range-check; add it to "+
				"cppIntWidths rather than assuming its width", pin.name, pin.cppType) {
			return
		}
		// The compiler narrows an out-of-range initializer silently, so a value
		// that does not fit is not the value segcore uses.
		if !assert.Zerof(t, got>>width,
			"%s does not fit its declared %s: segcore's %d truncates, so the constant "+
				"it uses is not the one written", pin.name, pin.cppType, got) {
			return
		}
		assert.Equalf(t, want, got,
			"%s = %d in segcore but %d in Go: these are one contract across the SDK, "+
				"the proxy and segcore, so fix the side that is wrong rather than this "+
				"check", pin.name, got, want)
	default:
		assert.Failf(t, "unsupported pin", "%s pins an unsupported value type %T", pin.name, want)
	}
}

// cppDeclarationsOf returns every declaration of name across the supplied
// normalised bodies, in source order.
//
// The name must be the declarator -- the identifier immediately before the `=`
// -- not merely mentioned. A derived limit such as
// `constexpr size_t kMaxBlobSize = kHeaderSize + kMaxBodySize;` is one
// declaration of kMaxBlobSize, and reading it as a second declaration of the
// two it references would report a duplicate that does not exist, on what is
// the most natural edit to make next to these constants.
func cppDeclarationsOf(bodies map[string]string, name string) []string {
	// `[^;]` cannot cross a statement boundary, so a match is one declaration,
	// and a use site -- which carries no constexpr of its own -- cannot produce
	// one.
	declaration := regexp.MustCompile(
		`(?:static\s+|inline\s+)*constexpr\s[^;]*?\b` + regexp.QuoteMeta(name) + `\s*=[^;]*;`)
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

// stripCppStd drops the std:: qualifier. <cstdint> and <cstddef> put these
// names in both the global and the std namespace, and a header that says
// std::size_t declares exactly what one saying size_t declares.
func stripCppStd(s string) string {
	return strings.ReplaceAll(s, "std::", "")
}

// canonicalCppDeclaration puts one already-normalised declaration into the
// spelling cppConstantPin.declarator produces, dropping only spelling the
// compiler ignores.
func canonicalCppDeclaration(decl string) string {
	decl = cppSpecifier.ReplaceAllString(normalizeCppWhitespace(strings.TrimSpace(decl)), "")
	decl = cppAssign.ReplaceAllString(stripCppStd(decl), " = ")
	return strings.ReplaceAll(decl, " ;", ";")
}

// evalCppIntExpr evaluates the small arithmetic these declarations use: decimal
// and hex literals with optional suffixes and C++14 digit separators, `a * b`,
// `a << b`, parentheses, and the `uint64_t{1}` brace-init form. Anything else
// is an error rather than a guess, which surfaces as "cannot evaluate".
func evalCppIntExpr(expr string) (uint64, error) {
	// The brace-init type tag carries a narrowing the compiler would apply, but
	// the declared type is pinned separately and the result range-checked
	// against it, so dropping the tag here cannot hide one.
	expr = strings.TrimSpace(cppBraceInit.ReplaceAllString(expr, "$1"))

	// Reduce innermost parenthesised groups so `(128 * 1024) * 1024` evaluates
	// instead of being reported as a divergence: .clang-format's 80-column wrap
	// makes adding a pair of parentheses a routine edit.
	for cppInnerParens.MatchString(expr) {
		var innerErr error
		expr = cppInnerParens.ReplaceAllStringFunc(expr, func(match string) string {
			value, err := evalCppIntExpr(match[1 : len(match)-1])
			if err != nil {
				innerErr = err
				return match
			}
			return strconv.FormatUint(value, 10)
		})
		if innerErr != nil {
			return 0, innerErr
		}
	}

	// `<<` binds looser than `*` in C++, so split on it first.
	if lhs, rhs, found := strings.Cut(expr, "<<"); found {
		if strings.Contains(rhs, "<<") {
			return 0, fmt.Errorf("unsupported chained shift in %q", expr)
		}
		left, err := evalCppIntExpr(lhs)
		if err != nil {
			return 0, err
		}
		right, err := evalCppIntExpr(rhs)
		if err != nil {
			return 0, err
		}
		if right >= 64 {
			return 0, fmt.Errorf("shift count %d is out of range in %q", right, expr)
		}
		return left << right, nil
	}

	product := uint64(1)
	for _, factor := range strings.Split(expr, "*") {
		value, err := parseCppIntegerToken(factor)
		if err != nil {
			return 0, fmt.Errorf("unsupported constant expression %q: %w", expr, err)
		}
		product *= value
	}
	return product, nil
}

func parseCppIntegerToken(token string) (uint64, error) {
	token = strings.TrimSpace(token)
	if !cppIntegerToken.MatchString(token) {
		return 0, fmt.Errorf("not an integer literal: %q", token)
	}
	token = strings.ReplaceAll(strings.TrimRight(token, "uUlL"), "'", "")
	return strconv.ParseUint(token, 0, 64)
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
