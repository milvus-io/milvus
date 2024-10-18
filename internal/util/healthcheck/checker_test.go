// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package healthcheck

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestChecker(t *testing.T) {
	expected1 := NewResult()
	expected1.AppendUnhealthyNodeMsg(UnhealthyNodeMsg{
		Role:         "dn",
		NodeID:       1,
		UnhealthyMsg: "msg1",
	})
	expected1.AppendUnhealthyCollectionMsgs(UnhealthyCollectionMsg{
		CollectionID: 1,
		UnhealthyMsg: "msg2",
	})

	checkFn := func() Result {
		return expected1
	}
	checker := NewChecker(100*time.Millisecond, checkFn)
	go checker.start()

	time.Sleep(150 * time.Millisecond)
	actual1 := checker.GetLatestCheckResult()
	assert.Equal(t, expected1, actual1)
	assert.False(t, actual1.IsHealthy())

	chr := GetCheckHealthResponseFrom(&actual1)
	assert.Equal(t, merr.Success(), chr.Status)
	assert.Equal(t, actual1.IsHealthy(), chr.IsHealthy)
	assert.Equal(t, 2, len(chr.Reasons))
	checker.Close()
}
