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

package externalrefresh

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// A refresh is priced at the fixed external-refresh budget: 1 cpu, 1 GiB.
func TestTaskResource_Refresh(t *testing.T) {
	paramtable.Init()
	refresh := &refreshExternalCollectionTask{}
	assert.Equal(t, taskresource.ExternalRefresh(), taskresource.Price(refresh.GetTaskResource()))
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 1024 * taskresource.MiB}, taskresource.Price(refresh.GetTaskResource()))
}
