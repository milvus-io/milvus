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
#include <stdlib.h>
#include "segcore/poc_updates_c.h"
*/
import "C"

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// JSONL format (publish the complete file by atomic rename):
// {"collection_id":123,"row_count":10000000,"operation_count":1,"field_ids":[101]}
// {"timestamp":<absolute HLC>,"primary_keys":[1,2],"assignments":[{"field_id":101,"value":null}]}
// Timestamps must be nondecreasing, later than every base row and no later
// than the wall-clock HLC upper bound. No query timestamp is rewritten: benchmark
// queries must use a snapshot at/after the applied-through timestamp in ready logs.
// This is a static local POC; WAL, replication and live update injection are absent.
type pocHeader struct {
	CollectionID   int64   `json:"collection_id"`
	RowCount       int64   `json:"row_count"`
	OperationCount int64   `json:"operation_count"`
	FieldIDs       []int64 `json:"field_ids"`
}

func (s *LocalSegment) PreparePOCUpdates(ctx context.Context) error {
	cfg := &paramtable.Get().QueryNodeCfg
	if cfg.POCUpdatesCollectionID.GetAsInt64() == 0 || cfg.POCUpdatesCollectionID.GetAsInt64() != s.Collection() || s.Type() != SegmentTypeSealed || s.Level() == datapb.SegmentLevel_L0 {
		return nil
	}
	if cfg.EnableSegmentPrune.GetAsBool() {
		return merr.WrapErrServiceInternalMsg("PK-update POC requires queryNode.enableSegmentPrune=false on all query runtimes")
	}
	if cfg.ExprResCacheEnabled.GetAsBool() {
		return merr.WrapErrServiceInternalMsg("PK-update POC requires queryNode.exprCache.enabled=false")
	}
	s.pocMu.Lock()
	defer s.pocMu.Unlock()
	if s.pocReady.Load() {
		return nil
	}
	path := cfg.POCUpdatesFile.GetValue()
	if path == "" || s.IsLazyLoad() {
		return merr.WrapErrServiceInternalMsg("PK-update POC requires an updates file and eager segment loading")
	}
	enabled := cfg.POCEnabledTempIndex.GetAsBool()
	log := mlog.With(mlog.FieldCollectionID(s.Collection()), mlog.FieldSegmentID(s.ID()), mlog.Bool("enabled_temp_index", enabled))
	log.Info(ctx, "PK-update POC preparing", mlog.String("updatesFile", path))
	var file *os.File
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		file, err = os.Open(path)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return merr.Wrap(err, "open POC updates file")
		}
		log.RatedWarn(ctx, 1.0/3, "PK-update POC file missing; retrying in 3 seconds", mlog.Err(err))
		timer := time.NewTimer(3 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return merr.Wrap(err, "read POC header")
		}
		return merr.WrapErrServiceInternalMsg("missing POC JSONL header")
	}
	var header pocHeader
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
		return merr.Wrap(err, "decode POC header")
	}
	if header.CollectionID != s.Collection() || header.RowCount <= 0 || header.OperationCount < 0 || len(header.FieldIDs) == 0 {
		return merr.WrapErrServiceInternalMsg("invalid POC header identity, counts or fields")
	}
	if err := s.PinIfNotReleased(); err != nil {
		return err
	}
	defer s.Unpin()
	var session unsafe.Pointer
	var base C.uint64_t
	started := time.Now()
	status := C.BeginPOCUpdates(s.ptr, (*C.int64_t)(unsafe.Pointer(&header.FieldIDs[0])), C.int64_t(len(header.FieldIDs)), C.int64_t(header.RowCount), C.bool(enabled), &session, &base)
	if err := HandleCStatus(ctx, &status, "build POC versioned store"); err != nil {
		return err
	}
	defer C.DestroyPOCUpdates(session)
	log.Info(ctx, "PK-update POC store built", mlog.Duration("buildDuration", time.Since(started)), mlog.Uint64("baseTimestamp", uint64(base)))
	s.logPOCStats(ctx, session, "initialized")
	applied, matched := int64(0), int64(0)
	last := uint64(base)
	applyStart := time.Now()
	// Physical milliseconds occupy the high 46 HLC bits. Allow all logical
	// ticks of the current millisecond without allocating a production TSO.
	maxTimestamp := tsoutil.ComposeTSByTime(time.Now().Add(time.Millisecond)) - 1
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if applied >= header.OperationCount {
			return merr.WrapErrServiceInternalMsg("POC file exceeds declared operation_count")
		}
		var op struct {
			Timestamp uint64 `json:"timestamp"`
		}
		line := scanner.Bytes()
		if err := json.Unmarshal(line, &op); err != nil {
			return merr.Wrap(err, "decode POC operation")
		}
		if op.Timestamp <= uint64(base) || op.Timestamp < last || op.Timestamp > maxTimestamp {
			return merr.WrapErrServiceInternalMsg("POC timestamps must increase after base data and must not be in the future")
		}
		var rows C.int64_t
		status = C.ApplyPOCUpdates(session, (*C.char)(unsafe.Pointer(&line[0])), C.int64_t(len(line)), &rows)
		if err := HandleCStatus(ctx, &status, "apply POC update"); err != nil {
			return err
		}
		applied++
		matched += int64(rows)
		last = op.Timestamp
	}
	if err := scanner.Err(); err != nil {
		return merr.Wrap(err, "read POC updates")
	}
	if applied != header.OperationCount {
		return merr.WrapErrServiceInternalMsg("POC file ended before declared operation_count")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	status = C.CommitPOCUpdates(session)
	if err := HandleCStatus(ctx, &status, "publish POC updates"); err != nil {
		return err
	}
	s.logPOCStats(ctx, session, "applied")
	s.pocReady.Store(true)
	log.Info(ctx, "PK-update POC ready", mlog.Int64("appliedCount", applied), mlog.Int64("matchedRows", matched), mlog.Uint64("appliedThroughTimestamp", last), mlog.Duration("applyDuration", time.Since(applyStart)), mlog.Bool("ready", true))
	return nil
}

func (s *LocalSegment) logPOCReadCounts(ctx context.Context) {
	if !s.pocReady.Load() {
		return
	}
	n := s.pocQueries.Inc()
	if n > 64 && n%100 != 0 {
		return
	}
	if err := s.PinIfNotReleased(); err != nil {
		return
	}
	defer s.Unpin()
	var indexReads, valueReads C.uint64_t
	status := C.GetPOCReadCounts(s.ptr, &indexReads, &valueReads)
	if err := HandleCStatus(ctx, &status, "read POC execution counters"); err != nil {
		return
	}
	s.logPOCStats(ctx, nil, "query")
	mlog.RatedInfo(ctx, 10, "PK-update POC reader usage", mlog.FieldSegmentID(s.ID()), mlog.Uint64("queryCount", n), mlog.Uint64("tempIndexReads", uint64(indexReads)), mlog.Uint64("valueReads", uint64(valueReads)))
}

func (s *LocalSegment) logPOCStats(ctx context.Context, session unsafe.Pointer, stage string) {
	var data *C.char
	status := C.GetPOCStats(session, s.ptr, &data)
	if err := HandleCStatus(ctx, &status, "read POC structure and path statistics"); err != nil {
		return
	}
	defer C.free(unsafe.Pointer(data))
	fields := []mlog.Field{mlog.FieldSegmentID(s.ID()), mlog.String("stage", stage), mlog.String("stats", C.GoString(data))}
	if stage == "query" {
		mlog.RatedInfo(ctx, 10, "PK-update POC statistics", fields...)
	} else {
		mlog.Info(ctx, "PK-update POC statistics", fields...)
	}
}
