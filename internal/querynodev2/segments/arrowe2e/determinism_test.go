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

package arrowe2e_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestFixtureIsDeterministic guards the differential test above.
//
// The reduce deduplicates by PK with timestamp tie-breaking. If the fixture puts
// the same PK in several segments with equal timestamps, the winner is arbitrary
// and the protobuf path differs from ITSELF run to run -- at which point
// comparing it against the Arrow path proves nothing and fails randomly. That is
// exactly what happened before the fixture gave each segment its own PK range,
// and the failure looked like an Arrow bug.
//
// Running the protobuf path twice and requiring equality keeps that from
// returning silently.
func TestFixtureIsDeterministic(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	setZeroCopy(t, false)

	for _, c := range []struct {
		name        string
		numSegments int
		rowsPerSeg  int
		hits        int
	}{
		{"4seg", 4, 1000, 500},
		{"8seg", 8, 500, 400},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t, c.numSegments, c.rowsPerSeg)
			defer f.release()
			node := requeryPlanNode(spreadPKs(c.hits, c.numSegments*c.rowsPerSeg))

			a, _ := f.runOnce(t, node)
			b, _ := f.runOnce(t, node)

			require.Equal(t, len(a.GetFieldsData()), len(b.GetFieldsData()))
			for i := range a.GetFieldsData() {
				require.True(t, proto.Equal(a.GetFieldsData()[i], b.GetFieldsData()[i]),
					"protobuf path is not deterministic at column %d (field %d); the "+
						"differential test cannot distinguish an Arrow bug from fixture noise",
					i, a.GetFieldsData()[i].GetFieldId())
			}
		})
	}
}
