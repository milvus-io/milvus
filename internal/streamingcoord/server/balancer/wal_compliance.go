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

package balancer

import (
	"context"
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// CheckWALPlacement reports each involved resource group's WAL placement obligation.
// Empty reasons mean ready; the empty group name carries an unattributable failure.
// The complete channel view includes uninitialized, assigning and unavailable channels,
// which are deliberately absent from the assignment-discovery Relations list.
func CheckWALPlacement(ctx context.Context, b Balancer, primaryRG string) (map[string]string, error) {
	result := make(map[string]string)
	if primaryRG == "" {
		return result, nil
	}
	assignment, err := b.GetLatestChannelAssignment()
	if err != nil {
		return nil, err
	}
	if assignment.PChannelView == nil {
		return nil, merr.WrapErrServiceUnavailableMsg("streaming channel view is not initialized")
	}
	nodes, err := b.GetAllStreamingNodes(ctx)
	if err != nil {
		return nil, err
	}
	result[primaryRG] = ""
	record := func(rg, reason string) {
		if result[rg] == "" {
			result[rg] = reason
		}
	}
	ids := make([]types.ChannelID, 0, len(assignment.PChannelView.Channels))
	for id := range assignment.PChannelView.Channels {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		ch := assignment.PChannelView.Channels[id]
		if ch.ChannelInfo().AccessMode != types.AccessModeRW {
			continue
		}
		node, known := nodes[ch.CurrentServerID()]
		if ch.IsAssigned() && known && node.ResourceGroup == primaryRG {
			continue
		}
		reason := fmt.Sprintf("WAL placement: pchannel %s is %s, expected assigned in primary rg=%s", ch.Name(), ch.State(), primaryRG)
		record(primaryRG, reason)
		// The current owner and every unacknowledged old owner have migration
		// obligations. Histories are cleared only once assignment completes.
		if known {
			record(node.ResourceGroup, reason)
		} else if ch.CurrentServerID() != 0 {
			record("", reason)
		}
		for _, previous := range ch.AssignHistories() {
			if previous.Channel.AccessMode != types.AccessModeRW {
				continue
			}
			if owner, ok := nodes[previous.Node.ServerID]; ok {
				record(owner.ResourceGroup, reason)
			} else {
				record("", reason)
			}
		}
	}
	return result, nil
}
