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

// Command genmrb1limits writes segcore's copy of the MRB1 envelope and
// admission limits from the Go ones.
//
// A wire-format limit that segcore enforces and Go enforces is one contract
// with two compilers, and nothing in the build linked them: a value edited on
// one side alone compiled and passed CI, and the symptom showed up in
// production as the proxy admitting a blob every querynode refuses, or the SDK
// refusing to build one the cluster would have taken.
//
// Rather than check the two copies against each other, this removes the second
// copy. RoaringMembership.h aliases the names in the generated header, so its
// constants are not values that could diverge -- there is one definition, in
// Go, and C++ reads it. What used to be a Go test parsing C++ declarations is
// now the C++ compiler resolving a name.
//
// The Go values live in pkg/util/roaringfilter, the proxy validator, because
// that is the side segcore has to agree with: a blob only reaches a querynode
// after Validate admitted it. The SDK's copy (client/v3/membership/roaringfilter) is a
// separate module that must not depend on pkg, so it stays a copy, held to
// these values by TestClientBuiltBlobsPassProxyValidation.
//
// Regenerate with `make generate-cpp-constants`. TestGeneratedHeaderIsCurrent
// fails when the checked-in header does not match what this would write, so a
// Go-side edit without a regenerate is caught by `go test`, not by a reviewer.
//
// The output has to be clang-format clean, because `make cppcheck` reformats
// internal/core in place and fails on any diff. Keep the emitted lines short
// and unaligned so clang-format has nothing to say about them.
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
)

// outputPath is where the generated header lands, relative to the repository
// root. internal/core globs *.cc/*.cpp/*.c/*.cxx for its source lists, so a
// header needs no CMake change.
const outputPath = "internal/core/src/common/MRB1Limits.generated.h"

// rootSentinel proves the C++ tree is in the checkout. It must be something no
// reorganization of an individual source would remove.
const rootSentinel = "internal/core/CMakeLists.txt"

// constant is one MRB1 value, rendered as a C++ declaration.
//
// cppType is what segcore declared before it took these from here, so every
// comparison in RoaringMembership.cpp keeps the width it already had.
//
// It is rendered brace-initialized, `uint16_t kX{1}`, rather than `= 1`,
// because a type too narrow for its value is then a compile error rather than a
// silent truncation. Measured, not assumed: clang accepts
// `constexpr uint16_t kX = 262144` without a word and gives kX the value 0,
// while `constexpr uint16_t kX{262144}` is -Wc++11-narrowing, an error by
// default. Getting a row's type wrong here is the one way this table can be
// wrong in a way the values do not show, so it is worth the spelling.
type constant struct {
	cppType string
	name    string
	value   string
	comment string
}

// constants is the MRB1 contract. Adding one here and regenerating is what
// makes it exist for segcore; there is nowhere else to add it.
var constants = []constant{
	{
		cppType: "std::string_view",
		name:    "kMagic",
		value:   strconv.Quote(roaringfilter.Magic),
		comment: "The 4-byte MRB1 envelope magic.",
	},
	{
		cppType: "uint16_t",
		name:    "kVersion",
		value:   u64(uint64(roaringfilter.Version)),
		comment: "The MRB1 envelope version this implementation accepts.",
	},
	{
		cppType: "uint16_t",
		name:    "kFormatPortableRoaring64",
		value:   u64(uint64(roaringfilter.FormatPortableRoaring64)),
		comment: "The RoaringFormatSpec portable extension for 64-bit integers.",
	},
	{
		cppType: "size_t",
		name:    "kHeaderSize",
		value:   u64(roaringfilter.HeaderSize),
		comment: "Size in bytes of the MRB1 envelope header.",
	},
	{
		cppType: "size_t",
		name:    "kMaxBodySize",
		value:   u64(roaringfilter.MaxBodyBytes),
		comment: "Bounds an untrusted portable body. 128 MiB.",
	},
	{
		cppType: "uint64_t",
		name:    "kMaxHighContainerCount",
		value:   u64(roaringfilter.MaxHighContainerCount),
		comment: "Bounds the separately allocated Roaring32 children. 2^18.",
	},
	{
		cppType: "uint64_t",
		name:    "kMaxEstimatedDecodedBytes",
		value:   u64(roaringfilter.MaxEstimatedDecodedBytes),
		comment: "Bounds one decoded bitmap. 64 MiB.",
	},
	{
		cppType: "uint64_t",
		name:    "kEstimatedHighContainerOverheadBytes",
		value:   u64(roaringfilter.EstimatedHighContainerOverheadBytes),
		comment: "Charged per high-32 child in the decoded-size estimate.",
	},
	{
		cppType: "uint64_t",
		name:    "kEstimatedLowContainerOverheadBytes",
		value:   u64(roaringfilter.EstimatedLowContainerOverheadBytes),
		comment: "Charged per Roaring32 container in the decoded-size estimate.",
	},
}

func u64(v uint64) string {
	return strconv.FormatUint(v, 10)
}

// header is what the generated file must contain, byte for byte.
func header() []byte {
	var out bytes.Buffer
	out.WriteString(`// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by cmd/tools/genmrb1limits. DO NOT EDIT.
//
// The MRB1 envelope and admission limits, generated from the Go proxy
// validator's constants (pkg/util/roaringfilter). segcore and Go enforce one
// contract and nothing in the build links them, so the C++ side is not a second
// copy of these numbers: RoaringMembership.h aliases the names below, and
// changing a limit means changing the Go constant and regenerating.
//
// Regenerate with ` + "`make generate-cpp-constants`" + `.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace milvus::mrb1 {

`)
	for i, c := range constants {
		if i > 0 {
			out.WriteString("\n")
		}
		fmt.Fprintf(&out, "// %s\n", c.comment)
		fmt.Fprintf(&out, "inline constexpr %s %s{%s};\n", c.cppType, c.name, c.value)
	}
	out.WriteString("\n}  // namespace milvus::mrb1\n")
	return out.Bytes()
}

// repoRoot walks up looking for rootSentinel and returns the directory holding
// it. Walking beats resolving a fixed "../../.." because the latter ties this
// to the command's depth in the tree.
func repoRoot() (string, error) {
	start, err := filepath.Abs(".")
	if err != nil {
		return "", err
	}
	for dir := start; ; {
		if _, err := os.Stat(filepath.Join(dir, rootSentinel)); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("%s is nowhere above %s", rootSentinel, start)
		}
		dir = parent
	}
}

func main() {
	root, err := repoRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "genmrb1limits:", err)
		os.Exit(1)
	}
	path := filepath.Join(root, outputPath)
	// 0644 rather than the 0600 gosec asks for, and #nosec rather than a
	// suppression-free rewrite: this writes a source file that every build reads
	// and that is committed to the tree, so 0600 would leave the regenerating
	// developer with a file no other user on the machine can read, and unlike
	// every other source next to it. pkg/streaming/util/message/codegen does
	// the same for the same reason.
	if err := os.WriteFile(path, header(), 0o644); err != nil { // #nosec G306
		fmt.Fprintln(os.Stderr, "genmrb1limits:", err)
		os.Exit(1)
	}
	fmt.Println("wrote", outputPath)
}
