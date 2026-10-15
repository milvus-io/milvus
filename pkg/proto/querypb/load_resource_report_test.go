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

package querypb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLoadResourceReportPresenceOnWire(t *testing.T) {
	for _, present := range []bool{false, true} {
		response := &GetDataDistributionResponse{LoadResourceReportSupported: true}
		if present {
			response.LoadResourceReport = &QueryNodeLoadResourceReport{TieredEvictionEnabled: true}
		}
		wire, err := proto.Marshal(response)
		require.NoError(t, err)
		decoded := &GetDataDistributionResponse{}
		require.NoError(t, proto.Unmarshal(wire, decoded))
		require.Equal(t, present, decoded.GetLoadResourceReport() != nil)
		require.True(t, decoded.GetLoadResourceReportSupported(), "capability survives an unavailable snapshot")
		if present {
			require.True(t, decoded.GetLoadResourceReport().GetTieredEvictionEnabled())
		}
	}
}
