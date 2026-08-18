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

	serverroaring "github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
)

// The MRB1 envelope and admission constants are one contract across three
// implementations: the SDK builder (client/v3/roaringfilter), the proxy
// validator (pkg/v3/util/roaringfilter) and segcore
// (internal/core/src/common/RoaringMembership.*). The SDK pre-rejects what the
// proxy would reject so a caller fails locally instead of after a round trip,
// and the proxy pre-rejects what segcore would reject so a hostile blob is not
// fanned out.
//
// The two Go copies are pinned to each other by
// TestClientBuiltBlobsPassProxyValidation, in this package because it is the
// only one that imports both modules. This pins the C++ copy, and lives beside
// it for the same reason: segcore is in this module, so reading its sources
// crosses no boundary.
//
// segcore does consume its own copies -- RoaringMembership.cpp reads all of
// them -- but nothing pinned them to the Go values, so editing
// kMaxHighContainerCount there alone compiles and passes CI. The symptom shows
// up in production as the proxy admitting a blob every querynode refuses, or
// the SDK refusing to build one the cluster would have taken.
var segcorePinnedConstants = []struct {
	cppName string
	goValue uint64
}{
	{"kVersion", uint64(serverroaring.Version)},
	{"kFormatPortableRoaring64", uint64(serverroaring.FormatPortableRoaring64)},
	{"kHeaderSize", serverroaring.HeaderSize},
	{"kMaxBodySize", serverroaring.MaxBodyBytes},
	{"kMaxHighContainerCount", serverroaring.MaxHighContainerCount},
	{"kMaxEstimatedDecodedBytes", serverroaring.MaxEstimatedDecodedBytes},
	{"kEstimatedHighContainerOverheadBytes", serverroaring.EstimatedHighContainerOverheadBytes},
	{"kEstimatedLowContainerOverheadBytes", serverroaring.EstimatedLowContainerOverheadBytes},
}

// segcoreUnpinnedConstants are names the completeness check tolerates. They are
// still listed, rather than the .cpp being left unscanned, so that a constant
// added there has to be classified instead of silently ignored.
var segcoreUnpinnedConstants = []string{
	// Checked below, as a string rather than an integer.
	"kMagic",

	// The portable-Roaring wire constants are deliberately not pinned here.
	// They are RoaringFormatSpec values rather than a Milvus contract: both
	// sides implement a published format, and two of them are derived rather
	// than chosen (kPortableBitmapBytes is 65536/8, and
	// kPortableRoaring64MinEntryBytes is the 4-byte high key plus the 8-byte
	// minimum child).
	//
	// Each side is instead held to the format behaviourally, by tests that were
	// written independently and exercise the value at its boundary rather than
	// reading it:
	//
	//   - the container-type threshold and the bitmap payload size: the Go
	//     validator accepts a CRoaring-written bitmap-container fixture
	//     (TestValidateAcceptsCRoaringGeneratedFixtures, "a bitmap container
	//     (5000 even values)") and segcore accepts the Go-written one
	//     (ParsesGoGeneratedBitmapFixture). Moving the threshold makes either
	//     side read a 5000-entry array where the bytes hold an 8192-byte bitmap,
	//     so the length stops adding up;
	//   - the run-cookie offset threshold: AcceptsValidPortableContainerEncodings
	//     asserts both sides of it, three containers with no offset table and
	//     four with one;
	//   - the cookies and the 8-byte Roaring64 prefix appear in every fixture;
	//   - the minimum-entry bound: RejectsUnsupportedRoaring32CookieAfterPrefixBound
	//     pads to exactly that minimum and asserts the failure is the cookie
	//     rather than the bound, so raising it fails.
	//
	// That is the stronger check, not a weaker one. Comparing literals cannot
	// see a use site, so flipping a `>` to a `>=` would pass this test while the
	// fixtures above fail. What it does give up is symmetry: these tests hold
	// each implementation to the format, not to each other, so a divergence
	// would have to slip past both suites rather than past one text comparison.
	"kPortableCookieNoRun",
	"kPortableCookieRun",
	"kPortableArrayMaxCardinality",
	"kPortableBitmapBytes",
	"kPortableNoOffsetThreshold",
	"kPortableRoaring64PrefixBytes",
	"kPortableRoaring64MinEntryBytes",
}

var segcoreSources = []string{
	"internal/core/src/common/RoaringMembership.h",
	"internal/core/src/common/RoaringMembership.cpp",
}

// TestRoaringSegcoreConstantsMatch fails when an MRB1 constant is changed on one
// side of the cgo boundary only.
//
// Reading the sources as text is crude, but it has no build dependency, so it
// runs in the same `go test` invocation that already gates the two Go copies.
// What it does not cover is worth knowing before trusting it:
//
//   - it reads declarations, never use sites. Changing `/
//     kPortableRoaring64MinEntryBytes` to `/ 16`, or a `>=` to a `>`, diverges
//     segcore from the Go validator exactly as a changed constant would, and
//     passes. This is the widest gap: the pin says the two sides agree on what
//     the numbers are, not on what they are used for;
//   - the duplicate rule below counts only declarations the regex can see, so a
//     live declaration in a form it cannot parse combined with a same-named
//     decoy it can parse would read the decoy. Either edit alone fails loudly.
//
// Generating the C++ constants from the Go ones, or static_asserting them
// against a generated header, would remove both gaps; that is the upgrade path
// if this ever costs someone real time.
func TestRoaringSegcoreConstantsMatch(t *testing.T) {
	values, unparsed := parseSegcoreIntConstants(t)

	for _, c := range segcorePinnedConstants {
		got, ok := values[c.cppName]
		if !ok {
			// Three different repairs, so say which one. Conflating them sends
			// the reader looking for a rename that never happened.
			if err, declared := unparsed[c.cppName]; declared {
				assert.Failf(t, "initializer not understood",
					"segcore declares %s but this test cannot evaluate its declaration: %v.\n"+
						"The value may well be correct -- teach the parser the new form "+
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

	assert.Equalf(t, serverroaring.Magic, parseSegcoreStringConstants(t)["kMagic"],
		"MRB1 magic diverged between Go and segcore")

	// Everything above checks constants this test already knows about. This is
	// the other direction: a constant added to segcore is invisible until
	// someone classifies it, which is the quiet way this test erodes. It covers
	// every k-name the sources declare, not only the ones parsed as integers, so
	// a new string constant, or one with a type the value parser cannot read, is
	// caught too.
	known := slices.Clone(segcoreUnpinnedConstants)
	for _, c := range segcorePinnedConstants {
		known = append(known, c.cppName)
	}

	for _, name := range declaredSegcoreConstants(t) {
		assert.Containsf(t, known, name,
			"segcore declares %s and nothing classifies it; add it to "+
				"segcorePinnedConstants, or to segcoreUnpinnedConstants with the "+
				"reason it needs no Go counterpart", name)
	}
}

var (
	// segcoreConstantName matches any k-prefixed constexpr identifier whatever
	// its type spelling. The completeness check must see declarations the value
	// parser cannot read -- those are exactly the ones that would otherwise slip
	// in.
	segcoreConstantName = regexp.MustCompile(`constexpr\s[^;=]*?\b(k[A-Z]\w*)\s*[={]`)
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

// parseSegcoreIntConstants returns the integer constants it could evaluate and,
// separately, the ones it recognized but could not, so a caller can tell "the
// source dropped this name" from "this test cannot read this declaration".
//
// Two properties matter more than the parsing itself, because without them a
// value change slips through while the test still passes:
//
//   - comments are stripped first, so leaving the old declaration behind in a
//     "// was: ..." comment cannot shadow the live one;
//   - a name declared twice fails instead of the last match winning, which also
//     catches #ifdef alternatives and a redeclaration in a nested scope.
func parseSegcoreIntConstants(t *testing.T) (map[string]uint64, map[string]error) {
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
			// Narrow to the declared type, because the compiler does. An
			// unbraced initializer that overflows its declaration is a silent
			// conversion, not an error, so `uint16_t kX = uint64_t{1} << 18`
			// really is 0 and reading the initializer alone would report 262144
			// and pass.
			narrowed, err := narrowToCppType(declaredType, value)
			if err != nil {
				unparsed[name] = err
				continue
			}
			values[name] = narrowed
		}
	}

	for _, name := range slices.Sorted(maps.Keys(declaredIn)) {
		require.Lenf(t, declaredIn[name], 1,
			"%s is declared %d times across %v; this test cannot tell which one the "+
				"compiler takes, so it would silently grade against the wrong value",
			name, len(declaredIn[name]), declaredIn[name])
	}
	require.NotEmpty(t, values, "parsed no integer constants from segcore")
	return values, unparsed
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
// mistaken for a live one. It does not model string literals containing comment
// markers, which these sources do not have.
func stripCppComments(body string) string {
	return cppComment.ReplaceAllString(body, " ")
}

// segcoreSentinel proves segcore is in the tree. It must be something no
// reorganisation of RoaringMembership.* would remove.
const segcoreSentinel = "internal/core/CMakeLists.txt"

// mentionedInSegcore reports whether the name appears as a whole word, which
// separates "renamed or deleted" from "declared in a form cppIntConstant cannot
// parse". Whole word, not substring: renaming kMaxBodySize to kMaxBodySizeBytes
// would otherwise still "mention" the old name and send the reader off to widen
// a regex that is working fine.
func mentionedInSegcore(t *testing.T, name string) bool {
	t.Helper()
	word := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`)
	for _, source := range segcoreSources {
		if word.MatchString(stripCppComments(readSegcoreSource(t, source))) {
			return true
		}
	}
	return false
}

// readSegcoreSource reads a segcore source. This package and internal/core are
// the same module in the same checkout, so a source this test cannot find is a
// failure, not a reason to skip: an earlier revision of this pin skipped when it
// could not locate its sources and printed ok, which is the failure mode the pin
// exists to remove.
func readSegcoreSource(t *testing.T, source string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(findSegcoreRoot(t), source))
	require.NoErrorf(t, err,
		"%s is present but %s is not: if the file moved or was renamed, update "+
			"segcoreSources -- silently skipping would leave the Go and C++ "+
			"constants unpinned", segcoreSentinel, source)
	return string(body)
}

// findSegcoreRoot walks up looking for segcoreSentinel and returns the directory
// holding it. Walking beats resolving a fixed "../../.." because the latter ties
// the pin to this package's depth, so moving the package would break it.
func findSegcoreRoot(t *testing.T) string {
	t.Helper()
	start, err := filepath.Abs(".")
	require.NoError(t, err)
	for dir := start; ; {
		if _, err := os.Stat(filepath.Join(dir, segcoreSentinel)); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqualf(t, dir, parent,
			"%s is nowhere above %s, but segcore and this package are the same "+
				"module, so it has to be", segcoreSentinel, start)
		dir = parent
	}
}

// evalCppIntExpr evaluates the small arithmetic these declarations use: decimal
// and hex literals with optional integer suffixes, `a * b`, `a << b`, redundant
// parentheses, and the `uint64_t{1}` brace-init form. Anything else is an error
// rather than a guess, which surfaces as "cannot evaluate its declaration".
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
		narrowed, err := narrowToCppType(parts[1], value)
		if err != nil {
			braceErr = err
			return match
		}
		return strconv.FormatUint(narrowed, 10)
	})
	if braceErr != nil {
		return 0, fmt.Errorf("unsupported brace initializer in %q: %w", expr, braceErr)
	}

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

// cppIntWidths lists the declared types these constants are allowed to use. An
// unlisted type is an error rather than an assumption: assuming 64 bits would
// silently accept a narrowing, and assuming anything narrower would invent one.
// Both spellings, because <cstdint> puts these in namespace std and a header
// that says std::uint16_t narrows exactly as one that says uint16_t.
var cppIntWidths = map[string]uint{
	"uint16_t": 16, "std::uint16_t": 16,
	"uint32_t": 32, "std::uint32_t": 32,
	"uint64_t": 64, "std::uint64_t": 64,
	// size_t is 64-bit on every platform milvus builds for.
	"size_t": 64, "std::size_t": 64,
}

// narrowToCppType truncates value the way an assignment to declaredType would.
// The declared type is the case that matters: `uint16_t kX = uint64_t{1} << 18`
// compiles to 0 on every platform -- clang warns, but warns only -- so reading
// the initializer alone would report 262144 and pass.
func narrowToCppType(declaredType string, value uint64) (uint64, error) {
	width, ok := cppIntWidths[declaredType]
	if !ok {
		return 0, fmt.Errorf(
			"declared type %q is not one this test can width-check; add it to "+
				"cppIntWidths rather than assuming its width", declaredType)
	}
	if width >= 64 {
		return value, nil
	}
	return value & ((uint64(1) << width) - 1), nil
}

func parseCppIntegerToken(token string) (uint64, error) {
	token = strings.TrimSpace(token)
	if !cppIntegerToken.MatchString(token) {
		return 0, fmt.Errorf("not an integer literal: %q", token)
	}
	return strconv.ParseUint(strings.TrimRight(token, "uUlL"), 0, 64)
}
