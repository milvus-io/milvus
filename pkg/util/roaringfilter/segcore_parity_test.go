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

package roaringfilter

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

// The MRB1 constants live in three places: the SDK builder
// (client/v3/roaringfilter), this validator, and segcore. The two Go copies are
// pinned to each other by TestClientBuiltBlobsPassProxyValidation
// (internal/parser/planparserv2, the only package that imports both modules).
// These are the C++ copies. This test lives here rather than there so it can see
// the unexported wire constants without exporting them for a test's benefit.
// pinnedConstants is the whole contract. TestSegcoreConstantsMatch checks each
// entry against segcore and, separately, that segcore declares nothing else.
// Deriving that second check from this same list rather than from a hand-kept
// copy of the names is what stops a deleted row from silently unpinning a live
// constant.
var pinnedConstants = []struct {
	cppName string
	goValue uint64
}{
	// Envelope and admission limits (RoaringMembership.h).
	{"kVersion", uint64(Version)},
	{"kFormatPortableRoaring64", uint64(FormatPortableRoaring64)},
	{"kHeaderSize", HeaderSize},
	{"kMaxBodySize", MaxBodyBytes},
	{"kMaxHighContainerCount", MaxHighContainerCount},
	{"kMaxEstimatedDecodedBytes", MaxEstimatedDecodedBytes},
	{"kEstimatedHighContainerOverheadBytes", EstimatedHighContainerOverheadBytes},
	{"kEstimatedLowContainerOverheadBytes", EstimatedLowContainerOverheadBytes},
	// Portable-body wire constants (RoaringMembership.cpp). The last one is
	// the bound that stops an attacker-controlled high-container count from
	// driving an allocation, so it matters as much as the limits above.
	{"kPortableCookieNoRun", uint64(portableCookieNoRun)},
	{"kPortableCookieRun", uint64(portableCookieRun)},
	{"kPortableArrayMaxCardinality", uint64(portableArrayMaxCardinality)},
	{"kPortableBitmapBytes", uint64(portableBitmapBytes)},
	{"kPortableNoOffsetThreshold", uint64(portableNoOffsetThreshold)},
	{"kPortableRoaring64PrefixBytes", uint64(portableRoaring64PrefixBytes)},
	{"kPortableRoaring64MinEntryBytes", uint64(portableRoaring64MinEntryBytes)},
}

// unpinnedSegcoreConstants are names the completeness check tolerates because
// they are checked another way or have no Go counterpart.
var unpinnedSegcoreConstants = []string{"kMagic"}

var segcoreSources = []string{
	"internal/core/src/common/RoaringMembership.h",
	"internal/core/src/common/RoaringMembership.cpp",
}

// TestSegcoreConstantsMatch fails when a constant is changed on one side of the
// cgo boundary only.
//
// The constants are a three-way contract, not three independent declarations:
// the SDK pre-rejects what the proxy would reject so a caller fails locally
// instead of after a round trip, and the proxy pre-rejects what segcore would
// reject so a hostile blob is not fanned out. segcore does consume its own
// copies, but nothing pinned them to the Go values, so editing
// kMaxHighContainerCount alone compiles and passes CI; the symptom appears in
// production as the proxy admitting a blob every querynode refuses, or the SDK
// refusing to build one the cluster would accept.
//
// Reading the sources as text is deliberately crude, but it has no build
// dependency, so it runs in the same `go test` invocation that gates the Go
// halves. What it does not cover is worth knowing before trusting it:
//
//   - the duplicate rule below counts only declarations the regex can see, so a
//     live declaration in a form it cannot parse (a multi-word type such as
//     `unsigned long long`, or an enum) combined with a same-named decoy it can
//     parse will read the decoy. Either edit alone fails loudly;
//   - a declared type cppFixedWidths does not list is assumed to be 64-bit, so
//     narrowing to one of those, or to a local `using` alias, passes. The map
//     lists everything plausible for these constants; add to it rather than
//     assuming it is complete;
//   - it reads declarations, never use sites. Changing `/
//     kPortableRoaring64MinEntryBytes` to `/ 16`, or a `>=` to a `>`, diverges
//     segcore from the Go validator exactly as a changed constant would, and
//     passes. This is the widest gap: the pin says the two sides agree on what
//     the numbers *are*, not on what they are used for;
//   - a local `using` alias for a narrow type defeats the width lookup, since
//     the alias name is what appears in the declaration;
//   - renaming or moving segcoreSentinel itself makes this test skip, the same
//     way renaming a source used to before the sentinel existed. The sentinel is
//     only a cheaper thing to keep an eye on, not a proof.
//
// Values are truncated to their declared type, and to a braced initializer's
// type, rather than being read from the initializer text alone. The declared
// type is the case that matters: `uint16_t kX = uint64_t{1} << 18` compiles to 0
// on every platform -- clang warns, but warns only -- so reading the initializer
// would report 262144 and pass. The braced form `uint16_t{262144}` is narrowing and clang
// rejects it outright on Linux and MSYS -- internal/core/CMakeLists.txt gives
// those `-Wno-error -Wno-all`, which does not demote a DefaultError diagnostic
// -- but it does compile, to 0, under the APPLE branch's `-Wall
// -Wno-c++11-narrowing`, so it is worth modelling for local macOS builds.
//
// Generating the C++ constants from the Go ones would remove this whole class
// of gap; that is the upgrade path if it ever costs someone real time.
func TestSegcoreConstantsMatch(t *testing.T) {
	ints, unparsed, _ := parseSegcoreIntConstants(t)

	for _, c := range pinnedConstants {
		got, ok := ints[c.cppName]
		if !ok {
			// Three different repairs, so say which one. Conflating them sends
			// the reader looking for a rename that never happened.
			if err, declared := unparsed[c.cppName]; declared {
				assert.Failf(t, "initializer not understood",
					"segcore declares %s but this test cannot evaluate its initializer: %v.\n"+
						"The value may well be correct -- teach evalCppIntExpr the new form "+
						"rather than assuming a divergence.", c.cppName, err)
				continue
			}
			if mentionedInSegcore(t, c.cppName) {
				assert.Failf(t, "declaration not recognised",
					"segcore still mentions %s, but no declaration this test can parse "+
						"matches it -- a multi-word type such as `unsigned long long`, "+
						"`constexpr static` instead of `static constexpr`, or an enum. "+
						"Widen cppIntConstant rather than assuming a divergence.", c.cppName)
				continue
			}
			assert.Failf(t, "constant not found",
				"segcore no longer declares %s; keep the name or update this test", c.cppName)
			continue
		}
		// assert, not require: report every divergence in one run rather than
		// making the reader re-run the test once per constant.
		assert.Equalf(t, c.goValue, got,
			"%s = %d in segcore but %d in Go: the MRB1 constants are one contract "+
				"across the SDK, the proxy and segcore", c.cppName, got, c.goValue)
	}

	assert.Equalf(t, Magic, parseSegcoreStringConstants(t)["kMagic"],
		"MRB1 magic diverged between Go and segcore")

	// Everything above checks constants this test already knows about. This is
	// the other direction: a constant added to segcore is invisible until
	// someone pins it, which is the quiet way this test erodes. It covers every
	// k-name the sources declare, not only the ones parsed as integers, so a new
	// string constant, or one with a type the value parser cannot read, is
	// caught too.
	known := make([]string, 0, len(pinnedConstants)+len(unpinnedSegcoreConstants))
	for _, c := range pinnedConstants {
		known = append(known, c.cppName)
	}
	known = append(known, unpinnedSegcoreConstants...)

	for _, name := range declaredSegcoreConstants(t) {
		assert.Containsf(t, known, name,
			"segcore declares %s and nothing pins it to a Go value; add it to "+
				"pinnedConstants, or to unpinnedSegcoreConstants if it has no Go "+
				"counterpart", name)
	}
}

// segcoreConstantName matches any k-prefixed constexpr identifier whatever its
// type spelling. The completeness check must see declarations the value parser
// cannot read -- those are exactly the ones that would otherwise slip in.
var segcoreConstantName = regexp.MustCompile(`constexpr\s[^;=]*?\b(k[A-Z]\w*)\s*[={]`)

func declaredSegcoreConstants(t *testing.T) []string {
	t.Helper()
	seen := map[string]struct{}{}
	for _, source := range segcoreSources {
		body := stripCppComments(readSegcoreSource(t, source))
		for _, m := range segcoreConstantName.FindAllStringSubmatch(body, -1) {
			seen[m[1]] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

var (
	// A constexpr declaration, with or without `static` (the header declares
	// class members, the .cpp uses namespace scope), tolerating the line breaks
	// clang-format inserts: .clang-format sets ColumnLimit 80, so `constexpr`,
	// the type, the name and the initializer can each land on their own line.
	cppIntConstant = regexp.MustCompile(
		`(?:static\s+)?constexpr\s+([\w:<>_]+)\s+(k\w+)\s*=\s*([^;]+);`)
	// Accepts `= "MRB1";` and the brace-init `{"MRB1"};`, and captures the whole
	// initializer up to the `;` rather than the first literal: C++ concatenates
	// adjacent string literals, so `= "MRB1" "X"` is a five-byte view. Reading
	// only the first would report MRB1 and pass while every querynode rejected
	// every blob the SDK builds.
	cppStringConstant = regexp.MustCompile(
		`(?:static\s+)?constexpr\s+std::string_view\s+(k\w+)\s*(?:=|=?\s*\{)([^;]+);`)
	cppStringLiteral = regexp.MustCompile(`"([^"]*)"`)
	// One alternation, left to right, so neither kind can eat the other's
	// terminator: block-first lets a `//` inside a block swallow its `*/`, and
	// line-first lets a `//` mentioning /* open a block. Whichever opens
	// earliest wins, as in C++.
	cppComment      = regexp.MustCompile(`(?s)/\*.*?\*/|//[^\n]*`)
	cppBraceInit    = regexp.MustCompile(`(\w+)\s*\{\s*(\w+)\s*\}`)
	cppInnerParens  = regexp.MustCompile(`\(([^()]*)\)`)
	cppIntegerToken = regexp.MustCompile(`^(0[xX][0-9a-fA-F]+|\d+)[uUlL]*$`)
)

// parseSegcoreIntConstants returns the integer constants it could evaluate and,
// separately, the ones it recognized but could not, so a caller can tell "the
// source dropped this name" from "this test cannot read this initializer".
//
// Two properties matter more than the parsing itself, because without them a
// value change slips through while the test still passes:
//
//   - comments are stripped first, so leaving the old declaration behind in a
//     "// was: ..." comment cannot shadow the live one;
//   - a name declared twice fails instead of the last match winning, which also
//     catches #ifdef alternatives and a redeclaration in a nested scope.
func parseSegcoreIntConstants(t *testing.T) (map[string]uint64, map[string]error, map[string][]string) {
	t.Helper()
	strs := parseSegcoreStringConstants(t)

	values := map[string]uint64{}
	unparsed := map[string]error{}
	declaredIn := map[string][]string{}
	for _, source := range segcoreSources {
		body := stripCppComments(readSegcoreSource(t, source))
		for _, m := range cppIntConstant.FindAllStringSubmatch(body, -1) {
			declaredType, name, initializer := m[1], m[2], m[3]
			if _, isString := strs[name]; isString {
				continue
			}
			declaredIn[name] = append(declaredIn[name], source)
			value, err := evalCppIntExpr(initializer)
			if err != nil {
				unparsed[name] = err
				continue
			}
			// Truncate to the declared type, because the compiler does. An
			// unbraced initializer that overflows its declaration is a silent
			// conversion, not an error, on every platform -- so `uint16_t kX =
			// uint64_t{1} << 18` really is 0, and reading the initializer alone
			// would report 262144 and pass.
			values[name] = truncateToCppType(declaredType, value)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(declaredIn)) {
		require.Lenf(t, declaredIn[name], 1,
			"%s is declared %d times across %v; this test cannot tell which one the "+
				"compiler takes, so it would silently grade against the wrong value",
			name, len(declaredIn[name]), declaredIn[name])
	}
	require.NotEmpty(t, values, "parsed no integer constants from segcore")
	return values, unparsed, declaredIn
}

// parseSegcoreStringConstants carries the same duplicate rule as the integer
// path, and for the same reason: without it, changing kMagic in the header and
// leaving a decoy declaration at namespace scope in the .cpp lets the last
// textual match win, and the test passes on a magic that no longer matches. The
// compiler ignores the decoy -- the .cpp refers to RoaringMembership::kMagic --
// so nothing else would catch it either.
func parseSegcoreStringConstants(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	declaredIn := map[string][]string{}
	for _, source := range segcoreSources {
		body := stripCppComments(readSegcoreSource(t, source))
		for _, m := range cppStringConstant.FindAllStringSubmatch(body, -1) {
			declaredIn[m[1]] = append(declaredIn[m[1]], source)
			// Concatenate every literal in the initializer, as C++ does.
			value := ""
			for _, lit := range cppStringLiteral.FindAllStringSubmatch(m[2], -1) {
				value += lit[1]
			}
			out[m[1]] = value
		}
	}
	for _, name := range slices.Sorted(maps.Keys(declaredIn)) {
		require.Lenf(t, declaredIn[name], 1,
			"%s is declared %d times across %v; this test cannot tell which one the "+
				"compiler takes, so it would silently grade against the wrong value",
			name, len(declaredIn[name]), declaredIn[name])
	}
	return out
}

// stripCppComments removes comments so a commented-out declaration cannot be
// mistaken for a live one. Line comments go first: doing it the other way lets a
// `//` comment mentioning /* open a block that swallows every declaration until
// another comment mentions */, which would hide a live one. It does not model
// string literals containing comment markers, which these sources do not have.
func stripCppComments(body string) string {
	return cppComment.ReplaceAllString(body, " ")
}

// segcoreSentinel proves we are in a full milvus checkout rather than a pkg/v3
// module fetched as a dependency. It must be something no reorganisation of
// RoaringMembership.* would remove.
const segcoreSentinel = "internal/core/CMakeLists.txt"

// mentionedInSegcore reports whether the name appears at all, which separates
// "renamed or deleted" from "declared in a form cppIntConstant cannot parse".
func mentionedInSegcore(t *testing.T, name string) bool {
	t.Helper()
	for _, source := range segcoreSources {
		if strings.Contains(stripCppComments(readSegcoreSource(t, source)), name) {
			return true
		}
	}
	return false
}

func mustAbs(t *testing.T, path string) string {
	t.Helper()
	abs, err := filepath.Abs(path)
	require.NoError(t, err)
	return abs
}

// readSegcoreSource reaches outside this module into internal/core, which is
// meaningful in the monorepo and meaningless anywhere else.
//
// The skip is gated on the sentinel rather than on each file's own absence, and
// that distinction is the point: gating per file means renaming or splitting
// RoaringMembership.h turns this pin off and `go test` prints ok. Renaming a C++
// header is a routine refactor, so it has to fail loudly, and the test may stay
// quiet only where segcore is genuinely not checked out.
func readSegcoreSource(t *testing.T, source string) string {
	t.Helper()
	root, ok := findSegcoreRoot(t)
	if !ok {
		t.Skipf("%s is nowhere above %s, so segcore is not in this tree",
			segcoreSentinel, mustAbs(t, "."))
	}

	body, err := os.ReadFile(filepath.Join(root, source))
	require.NoErrorf(t, err,
		"%s is present but %s is not: if the file moved or was renamed, update "+
			"segcoreSources -- silently skipping would leave the Go and C++ "+
			"constants unpinned", segcoreSentinel, source)
	return string(body)
}

// findSegcoreRoot walks up looking for segcoreSentinel and returns the directory
// holding it.
//
// Two earlier versions of this got it wrong in the same way, so the rule is
// worth stating: the only thing allowed to make this test skip is segcore not
// being in the tree at all. Resolving "../../.." tied the pin to this package's
// depth, so moving the package skipped. Walking up for .git skipped in any
// checkout without one -- .dockerignore excludes .git, so that is every docker
// build context -- and stopped early at a nested .git. Both printed ok. Looking
// for the sentinel itself has no such failure mode: if it is anywhere above us
// the pin runs, and if it is nowhere then segcore genuinely is not here.
func findSegcoreRoot(t *testing.T) (string, bool) {
	t.Helper()
	dir, err := filepath.Abs(".")
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, segcoreSentinel)); err == nil {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

// evalCppIntExpr evaluates the small arithmetic these declarations use: decimal
// and hex literals with optional integer suffixes, `a * b`, `a << b`, redundant
// parentheses, and the `uint64_t{1}` brace-init form.
func evalCppIntExpr(expr string) (uint64, error) {
	expr = strings.Join(strings.Fields(expr), " ")

	var braceErr error
	expr = cppBraceInit.ReplaceAllStringFunc(expr, func(match string) string {
		parts := cppBraceInit.FindStringSubmatch(match)
		value, err := parseCppIntegerToken(parts[2])
		if err != nil {
			braceErr = err
			return match
		}
		// Model what the compiler does rather than dropping the type. A braced
		// initializer that does not fit is narrowing: a hard clang error under
		// the -Wno-error -Wno-all the LINUX and MSYS branches use, but permitted
		// by the APPLE branch's -Wall -Wno-c++11-narrowing, where it really does
		// compile to 0. Erasing the type here would report 262144 and pass.
		return strconv.FormatUint(truncateToCppType(parts[1], value), 10)
	})
	if braceErr != nil {
		return 0, fmt.Errorf("unsupported brace initializer in %q: %w", expr, braceErr)
	}

	// Reduce innermost parenthesised groups so `(128 * 1024) * 1024` evaluates
	// instead of being reported as a divergence.
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

	expr = strings.TrimSpace(expr)

	if parts := strings.Split(expr, "<<"); len(parts) == 2 {
		lhs, err := evalCppIntExpr(parts[0])
		if err != nil {
			return 0, err
		}
		rhs, err := evalCppIntExpr(parts[1])
		if err != nil {
			return 0, err
		}
		return lhs << rhs, nil
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

// truncateToCppType narrows value to the declared type's width. Types the map
// does not know are left alone: unknown means "wider than we can prove", and
// silently zeroing a constant we merely failed to recognise would be worse than
// missing a narrowing.
func truncateToCppType(declaredType string, value uint64) uint64 {
	if declaredType == "bool" {
		// Any nonzero converts to true, it does not truncate.
		if value != 0 {
			return 1
		}
		return 0
	}
	t, ok := cppFixedWidths[declaredType]
	if !ok || t.width >= 64 {
		return value
	}
	truncated := value & ((uint64(1) << t.width) - 1)
	if t.signed && truncated&(uint64(1)<<(t.width-1)) != 0 {
		// A negative value widens by sign extension when it is compared against
		// a 64-bit Go constant, so model that rather than reporting the raw
		// low bits.
		truncated |= ^((uint64(1) << t.width) - 1)
	}
	return truncated
}

// cppFixedWidths maps the fixed-width types these sources use to their bit
// count, so a value can be truncated the way C++ truncates it.
// Both spellings: <cstdint> puts these in namespace std, and a header that says
// std::uint16_t is as narrowing as one that says uint16_t. Missing the qualified
// form let `static constexpr std::uint16_t kMaxBodySize = 128 * 1024 * 1024`
// report 134217728 and pass while segcore saw 0.
var cppFixedWidths = map[string]cppIntType{
	"uint8_t": {8, false}, "std::uint8_t": {8, false},
	"int8_t": {8, true}, "std::int8_t": {8, true},
	"uint16_t": {16, false}, "std::uint16_t": {16, false},
	"int16_t": {16, true}, "std::int16_t": {16, true},
	"uint32_t": {32, false}, "std::uint32_t": {32, false},
	"int32_t": {32, true}, "std::int32_t": {32, true},
	"uint64_t": {64, false}, "std::uint64_t": {64, false},
	"int64_t": {64, true}, "std::int64_t": {64, true},
	// size_t is 64-bit on every platform milvus builds for.
	"size_t": {64, false}, "std::size_t": {64, false},
	// Spellings nobody would choose for these constants, but a narrowing in any
	// of them is as silent as one in uint16_t, and listing them is free.
	// Single-word spellings only: the type is captured by [\w:<>_]+, which cannot
	// span a space, so a multi-word type never reaches this map at all -- it
	// fails as an unrecognised declaration instead.
	// char's signedness and wchar_t's width are platform-dependent (char is
	// unsigned on aarch64 Linux, wchar_t is 16-bit on Windows). Both are
	// modelled at their narrowest plausible width: the point is to notice a
	// truncation, and too small a width can only over-report one, never miss it.
	// bool is a conversion rather than a truncation, handled separately.
	"bool": {1, false}, "char": {8, false}, "char8_t": {8, false},
	"wchar_t": {16, false},
	"short":   {16, true}, "unsigned": {32, false}, "int": {32, true},
	"char16_t": {16, false}, "char32_t": {32, false},
	"int_least16_t": {16, true}, "uint_least16_t": {16, false},
	"int_fast16_t": {16, true}, "uint_fast16_t": {16, false},
	"std::int_least16_t": {16, true}, "std::uint_least16_t": {16, false},
	"std::int_fast16_t": {16, true}, "std::uint_fast16_t": {16, false},
}

type cppIntType struct {
	width  uint
	signed bool
}

func parseCppIntegerToken(token string) (uint64, error) {
	token = strings.TrimSpace(token)
	if !cppIntegerToken.MatchString(token) {
		return 0, fmt.Errorf("not an integer literal: %q", token)
	}
	return strconv.ParseUint(strings.TrimRight(token, "uUlL"), 0, 64)
}
