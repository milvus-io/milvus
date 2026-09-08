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

package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/common"
)

// The feature usage report names an index parameter only if it is on the
// official-key allowlist in pkg/common; anything else is reported as _custom.
// The scalar index parameters defined here have no constant in pkg/common, so
// they are listed there as literals; this keeps the two in step.
func TestScalarIndexParamsAreOfficialFeatureKeys(t *testing.T) {
	for _, key := range []string{MinGramKey, MaxGramKey, FmSaSampleRateKey, FmBlockBytesKey} {
		assert.True(t, common.IsOfficialFeatureKey(key), key)
	}
}
