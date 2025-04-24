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

package index

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	errCancel      = errors.New("canceled")
	DiskUsageRatio = 4.0
)

type Key struct {
	ClusterID string
	TaskID    typeutil.UniqueID
}

type Task interface {
	Ctx() context.Context
	Name() string
	OnEnqueue(context.Context) error
	SetState(state indexpb.JobState, failReason string)
	GetState() indexpb.JobState
	PreExecute(context.Context) error
	Execute(context.Context) error
	PostExecute(context.Context) error
	Reset()
	GetSlot() int64
}
