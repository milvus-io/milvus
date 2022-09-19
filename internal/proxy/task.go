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

package proxy

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/api/commonpb"
)

const (
	AnnsFieldKey    = "anns_field"
	TopKKey         = "topk"
	SearchParamsKey = "params"
	RoundDecimalKey = "round_decimal"
	OffsetKey       = "offset"
	LimitKey        = "limit"

	// minFloat32 minimum float.
	minFloat32 = -1 * float32(math.MaxFloat32)
)

type task interface {
	TraceCtx() context.Context
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Name() string
	Type() commonpb.MsgType
	BeginTs() Timestamp
	EndTs() Timestamp
	SetTs(ts Timestamp)
	OnEnqueue() error
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
}

type dmlTask interface {
	task
	getChannels() ([]pChan, error)
	getPChanStats() (map[pChan]pChanStatistics, error)
}
