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

package simulation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimSmokeLoad(t *testing.T) {
	sim := New(t, Options{})
	sim.AddNodes(1, 4, DefaultNodeConfig())
	for id := int64(1); id <= 3; id++ {
		sim.LoadCollection(id, 1, 2, 8, 10_000)
	}
	require.True(t, sim.WaitUntil(30*time.Second, func() bool { return sim.Loaded(1, 2, 3) }), "collections did not load")

	copies := make(map[int64]int)
	for _, view := range sim.Fleet.Snapshot() {
		for id := range view.Segments {
			copies[id]++
		}
	}
	require.Len(t, copies, 3*2*8)
	for id, n := range copies {
		require.Equal(t, 1, n, "segment %d", id)
	}
}
