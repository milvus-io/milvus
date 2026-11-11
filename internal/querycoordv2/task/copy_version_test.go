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

package task

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An unsubscribe carries the version of the delegator it was planned against,
// so QueryNode can keep a delegator watched after the plan.
func TestUnsubRequestCarriesPlannedDelegatorVersion(t *testing.T) {
	replica := membershipReplica([]int64{1, 14}, nil, nil, nil)
	unsub := func(action *ChannelAction) int64 {
		task := newTestChannelTask(t, replica, action)
		return packUnsubDmChannelRequest(task, action).GetVersion()
	}
	require.Equal(t, int64(9), unsub(NewChannelReduceAction(1, membershipChannel, 9)))
	require.Zero(t, unsub(NewChannelAction(1, ActionTypeReduce, membershipChannel)), "an unplanned unsubscribe removes any delegator")
}
