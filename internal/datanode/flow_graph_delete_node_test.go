// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlowGraphDeleteNode_Operate_Nil(t *testing.T) {
	ctx := context.Background()
	var replica Replica
	deleteNode := newDeleteDNode(ctx, replica)
	result := deleteNode.Operate([]Msg{})
	assert.Equal(t, len(result), 0)
}

func TestFlowGraphDeleteNode_Operate_Invalid_Size(t *testing.T) {
	ctx := context.Background()
	var replica Replica
	deleteNode := newDeleteDNode(ctx, replica)
	var Msg1 Msg
	var Msg2 Msg
	result := deleteNode.Operate([]Msg{Msg1, Msg2})
	assert.Equal(t, len(result), 0)
}
