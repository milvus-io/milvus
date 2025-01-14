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

package compaction

import (
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

//go:generate mockery --name=Compactor --structname=MockCompactor --output=./  --filename=mock_compactor.go --with-expecter --inpackage
type Compactor interface {
	Complete()
	Compact() (*datapb.CompactionPlanResult, error)
	Stop()
	GetPlanID() typeutil.UniqueID
	GetCollection() typeutil.UniqueID
	GetChannelName() string
	GetCompactionType() datapb.CompactionType
	GetSlotUsage() int64
}
