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

package segments

/*
#cgo pkg-config: milvus_core

#include "segcore/load_index_c.h"
#include "common/binary_set_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"time"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// LoadIndexInfo is a wrapper of the underlying C-structure C.CLoadIndexInfo
type LoadIndexInfo struct {
	cLoadIndexInfo C.CLoadIndexInfo
}

// newLoadIndexInfo returns a new LoadIndexInfo and error
func newLoadIndexInfo(ctx context.Context) (*LoadIndexInfo, error) {
	var cLoadIndexInfo C.CLoadIndexInfo

	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		status = C.NewLoadIndexInfo(&cLoadIndexInfo)
		return nil, nil
	}).Await()
	if err := HandleCStatus(ctx, &status, "NewLoadIndexInfo failed"); err != nil {
		return nil, err
	}
	return &LoadIndexInfo{cLoadIndexInfo: cLoadIndexInfo}, nil
}

// deleteLoadIndexInfo would delete C.CLoadIndexInfo
func deleteLoadIndexInfo(info *LoadIndexInfo) {
	GetDynamicPool().Submit(func() (any, error) {
		C.DeleteLoadIndexInfo(info.cLoadIndexInfo)
		return nil, nil
	}).Await()
}

func (li *LoadIndexInfo) cleanLocalData(ctx context.Context) error {
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		status = C.CleanLoadedIndex(li.cLoadIndexInfo)
		return nil, nil
	}).Await()
	return HandleCStatus(ctx, &status, "failed to clean cached data on disk")
}

func (li *LoadIndexInfo) appendLoadIndexInfo(ctx context.Context, info *cgopb.LoadIndexInfo) error {
	marshaled, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	var status C.CStatus
	_, _ = GetDynamicPool().Submit(func() (any, error) {
		status = C.FinishLoadIndexInfo(li.cLoadIndexInfo, (*C.uint8_t)(unsafe.Pointer(&marshaled[0])), (C.uint64_t)(len(marshaled)))
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "FinishLoadIndexInfo failed")
}

func (li *LoadIndexInfo) loadIndex(ctx context.Context) error {
	var status C.CStatus
	_, _ = GetLoadPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				paramtable.GetStringNodeID(),
				"AppendIndexV2",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		traceCtx := ParseCTraceContext(ctx)
		status = C.AppendIndexV2(traceCtx.ctx, li.cLoadIndexInfo)
		runtime.KeepAlive(traceCtx)
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "AppendIndexV2 failed")
}
