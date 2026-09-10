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

//go:build test

package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/plan_c.h"
*/
import "C"

import (
	"testing"
	"unsafe"
)

var dummyPtrSentinel byte

// dummyUnsafePtr returns a non-nil unsafe.Pointer sentinel for test-only
// use. The pointer must NOT be dereferenced — it only satisfies
// non-nil checks in Go-side validation paths that reject before any CGO
// dereference happens.
func dummyUnsafePtr() unsafe.Pointer {
	return unsafe.Pointer(&dummyPtrSentinel)
}

// NewDummySearchPlanForTest returns a SearchPlan with a non-nil (but
// bogus) cSearchPlan pointer, so tests can exercise Go-side validation
// beyond the plan-nil guard without setting up a full collection + schema.
// The returned plan must NOT be used for any C++ call that actually
// dereferences cSearchPlan — only for code paths that check nil before use.
func NewDummySearchPlanForTest(t *testing.T) *SearchPlan {
	t.Helper()
	return &SearchPlan{
		cSearchPlan: C.CSearchPlan(dummyUnsafePtr()),
	}
}

// NewDummyPlaceholderGroupForTest returns a non-nil bogus placeholder-group
// pointer for tests that exercise Go-side validation beyond the placeholder-
// group-nil guard. Must NOT be dereferenced.
func NewDummyPlaceholderGroupForTest() unsafe.Pointer {
	return dummyUnsafePtr()
}
