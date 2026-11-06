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

package dml

import (
	"context"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// partialUpsertState owns original row identities across channel-local attempts.
// rows indexes the immutable payload; writeRows indexes this attempt's input.
type partialUpsertState struct {
	resultOrder     []int
	rows            []int
	writeRows       []int
	originalCount   uint32
	written         map[int]any
	retryRows       []int
	timestamp       uint64
	insertCount     int64
	deleteCount     int64
	appendAttempted bool
}

func (it *UpsertTask) initPartialState() {
	rows := make([]int, it.req.NumRows)
	for i := range rows {
		rows[i] = i
	}
	it.partialState = &partialUpsertState{rows: rows, originalCount: it.req.NumRows, written: make(map[int]any)}
}

func (it *UpsertTask) originalPartialRow(row int) int {
	if it.partialState == nil {
		return row
	}
	return it.partialState.rows[row]
}

func (it *UpsertTask) attemptAllocatedIDs() map[int]int64 {
	if it.partialState == nil {
		return it.partialUpdateAllocatedIDs
	}
	ids := make(map[int]int64)
	for row, original := range it.partialState.rows {
		if id, ok := it.partialUpdateAllocatedIDs[original]; ok {
			ids[row] = id
		}
	}
	return ids
}

func (it *UpsertTask) validateWriteCondition() error {
	c := it.req.GetWriteCondition()
	if c == nil {
		return nil
	}
	if c.Op != milvuspb.WriteCondition_GT && c.Op != milvuspb.WriteCondition_GTE {
		return merr.WrapErrParameterInvalidMsg("write_condition.op must be GT or GTE")
	}
	if c.OnMissing < milvuspb.WriteCondition_INSERT_IF_MISSING || c.OnMissing > milvuspb.WriteCondition_ERROR_IF_MISSING {
		return merr.WrapErrParameterInvalidMsg("invalid write_condition.on_missing")
	}
	field, err := it.schema.SchemaHelper.GetFieldFromName(c.Field)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("write_condition field %q does not exist", c.Field)
	}
	if field.IsPrimaryKey || field.IsPartitionKey || field.IsClusteringKey || field.IsDynamic || typeutil.IsStructSubField(c.Field) || !typeutil.IsIntegerType(field.DataType) {
		return merr.WrapErrParameterInvalidMsg("write_condition requires an ordinary integer field")
	}
	present := false
	for _, data := range it.req.FieldsData {
		if data.GetFieldName() == c.Field {
			present = true
		}
	}
	if !present {
		return merr.WrapErrParameterInvalidMsg("write_condition field %q must be supplied", c.Field)
	}
	for _, op := range it.req.FieldOps {
		if op.Op != schemapb.FieldPartialUpdateOp_REPLACE && (op.FieldName == c.Field || c.Op == milvuspb.WriteCondition_GTE) {
			return merr.WrapErrParameterInvalidMsg("write_condition is incompatible with field operation on %s", op.FieldName)
		}
	}
	it.req.PartialUpdate = true
	return nil
}

// selectPartialFields gathers the immutable request's compact payload separately
// from logical-row validity. The normal preparation path fills NULLs/defaults once.
func selectPartialFields(fields []*schemapb.FieldData, rows []int) []*schemapb.FieldData {
	out := make([]*schemapb.FieldData, len(fields))
	logical := make([]int64, len(rows))
	for i, row := range rows {
		logical[i] = int64(row)
	}
	for i, field := range fields {
		if field.GetStructArrays() != nil {
			sub := selectPartialFields(field.GetStructArrays().Fields, rows)
			out[i] = &schemapb.FieldData{FieldName: field.FieldName, FieldId: field.FieldId, Type: field.Type, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: sub}}}
			continue
		}
		valid := typeutil.GetFieldDataValidData(field)
		dst := typeutil.PrepareResultFieldData([]*schemapb.FieldData{field}, int64(len(rows)))[0]
		if len(valid) > 0 {
			// Physical offsets depend only on validity, for scalars, defaults,
			// regular vectors and ArrayOfVector struct children alike.
			offsets, _ := typeutil.BuildNullableVectorDataIndices(valid)
			indices := make([]int64, 0, len(rows))
			for _, row := range rows {
				if valid[row] {
					indices = append(indices, int64(offsets[row]))
				}
			}
			typeutil.AppendFieldDataByColumn(dst, field, indices, logical)
		} else {
			typeutil.AppendFieldDataByColumn(dst, field, logical)
		}
		out[i] = dst
	}
	return out
}

func conditionInt(field *schemapb.FieldData, row int) (int64, bool, error) {
	if field == nil {
		return 0, false, merr.WrapErrServiceInternalMsg("version field missing from query or request")
	}
	valid := typeutil.GetFieldDataValidData(field)
	if len(valid) > 0 {
		if row >= len(valid) {
			return 0, false, merr.WrapErrServiceInternalMsg("version validity is not row aligned")
		}
		if !valid[row] {
			return 0, false, nil
		}
	}
	values := field.GetScalars()
	valueCount := len(values.GetIntData().GetData())
	if field.Type == schemapb.DataType_Int64 {
		valueCount = len(values.GetLongData().GetData())
	}
	if len(valid) > 0 && valueCount != len(valid) {
		logical := row
		row = 0
		for _, present := range valid[:logical] {
			if present {
				row++
			}
		}
	}
	if row >= valueCount {
		return 0, false, merr.WrapErrServiceInternalMsg("version values are not row aligned")
	}
	if field.Type == schemapb.DataType_Int64 {
		return values.GetLongData().GetData()[row], true, nil
	}
	return int64(values.GetIntData().GetData()[row]), true, nil
}

func (it *UpsertTask) selectConditionalRows(ids, oldIDs *schemapb.IDs, oldFields []*schemapb.FieldData, updates, inserts []int) ([]int, []int, error) {
	s := it.partialState
	for _, meta := range it.partialUpdateCASGroups {
		if meta.ReadTs > s.timestamp {
			s.timestamp = meta.ReadTs
		}
	}
	c := it.req.GetWriteCondition()
	conditionNotMet, missingSkipped := 0, 0
	if c != nil {
		var incoming, stored *schemapb.FieldData
		for _, field := range it.upsertMsg.InsertMsg.FieldsData {
			if field.FieldName == c.Field {
				incoming = field
			}
		}
		for _, field := range oldFields {
			if field.FieldName == c.Field {
				stored = field
			}
		}
		for row := range s.rows {
			_, valid, err := conditionInt(incoming, row)
			if err != nil {
				return nil, nil, err
			}
			if !valid {
				return nil, nil, merr.WrapErrParameterInvalidMsg("write_condition incoming version is NULL at row %d", s.rows[row])
			}
		}
		if c.OnMissing == milvuspb.WriteCondition_ERROR_IF_MISSING && len(inserts) > 0 {
			missing := make([]any, 0, len(inserts))
			for _, row := range inserts {
				missing = append(missing, typeutil.GetPK(ids, int64(row)))
			}
			return nil, nil, merr.WrapErrParameterInvalidMsg("write_condition primary keys do not exist: %v", missing)
		}
		positions := make(map[any]int)
		for row := 0; row < typeutil.GetSizeOfIDs(oldIDs); row++ {
			pk := typeutil.GetPK(oldIDs, int64(row))
			if _, ok := positions[pk]; ok {
				return nil, nil, merr.WrapErrServiceInternalMsg("duplicate queried primary key %v", pk)
			}
			positions[pk] = row
		}
		selected := make([]int, 0, len(updates))
		for _, row := range updates {
			newVersion, _, err := conditionInt(incoming, row)
			if err != nil {
				return nil, nil, err
			}
			oldVersion, valid, err := conditionInt(stored, positions[typeutil.GetPK(ids, int64(row))])
			if err != nil {
				return nil, nil, err
			}
			if !valid || newVersion > oldVersion || (c.Op == milvuspb.WriteCondition_GTE && newVersion == oldVersion) {
				selected = append(selected, row)
			} else {
				conditionNotMet++
			}
		}
		updates = selected
		if c.OnMissing == milvuspb.WriteCondition_SKIP_IF_MISSING {
			missingSkipped = len(inserts)
			inserts = nil
		}
	}
	it.deletePKs = &schemapb.IDs{}
	for _, row := range updates {
		typeutil.AppendIDs(it.deletePKs, ids, row)
	}
	s.writeRows = append(append(make([]int, 0, len(updates)+len(inserts)), updates...), inserts...)
	if s.resultOrder == nil {
		for _, row := range s.writeRows {
			s.resultOrder = append(s.resultOrder, s.rows[row])
		}
	}
	// Publish skips only after this selection is complete. These rows are now
	// terminal even if later preparation or another channel fails.
	it.observeConditionalOutcome("condition_not_met", conditionNotMet)
	it.observeConditionalOutcome("missing_skipped", missingSkipped)
	return updates, inserts, nil
}

func (it *UpsertTask) selectedPartialIDs(ids *schemapb.IDs) *schemapb.IDs {
	if it.partialState == nil || it.partialState.writeRows == nil {
		return ids
	}
	selected := &schemapb.IDs{}
	for _, row := range it.partialState.writeRows {
		typeutil.AppendIDs(selected, ids, row)
	}
	return selected
}

func conditionalExecutionError(err error) error {
	if merr.Code(err) == merr.Code(merr.ErrCollectionConditionalUpsertExecution) {
		return err
	}
	return merr.WrapErrCollectionConditionalUpsertExecutionErr(err, "conditional upsert may have partially committed; automatic replay is unsafe")
}

func (it *UpsertTask) publishPartialResult() {
	s := it.partialState
	indexes := make([]int, 0, len(s.written))
	for row := range s.written {
		indexes = append(indexes, row)
	}
	sort.Ints(indexes)
	ids := &schemapb.IDs{}
	pk, _ := typeutil.GetPrimaryFieldSchema(it.schema.CollectionSchema)
	if it.req.GetWriteCondition() == nil && !pk.GetAutoID() && s.resultOrder != nil {
		indexes = indexes[:0]
		for _, row := range s.resultOrder {
			if _, ok := s.written[row]; ok {
				indexes = append(indexes, row)
			}
		}
	}
	if pk.DataType == schemapb.DataType_Int64 {
		ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}
	} else {
		ids.IdField = &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{}}}
	}
	succ := make([]uint32, 0, len(indexes))
	for _, row := range indexes {
		if pk.DataType == schemapb.DataType_Int64 {
			ids.GetIntId().Data = append(ids.GetIntId().Data, s.written[row].(int64))
		} else {
			ids.GetStrId().Data = append(ids.GetStrId().Data, s.written[row].(string))
		}
		if it.req.GetWriteCondition() == nil {
			succ = append(succ, uint32(len(succ)))
		} else {
			succ = append(succ, uint32(row))
		}
	}
	it.result.IDs = ids
	it.result.SuccIndex = succ
	it.result.UpsertCnt = int64(len(indexes))
	it.result.InsertCnt = s.insertCount
	it.result.DeleteCnt = s.deleteCount
	it.result.Timestamp = s.timestamp
}

// retainRejectedRows rebuilds only original rows whose destination channel aborted.
func (it *UpsertTask) retainRejectedRows(ctx context.Context) error {
	s := it.partialState
	s.rows = append([]int(nil), s.retryRows...)
	sort.Ints(s.rows)
	it.partialUpdateResultIDs = nil
	return it.preparePartialUpdateRetryAttempt(ctx)
}

// AppendResponses is aligned with input messages, including all chunks of a
// channel transaction. Reduce by channel before publishing any row result.
func (it *UpsertTask) recordPartialAppend(messages []message.MutableMessage, resp streaming.AppendResponses) error {
	s := it.partialState
	if len(messages) != len(resp.Responses) {
		return merr.WrapErrServiceInternalMsg("append response count mismatch")
	}
	outcomes := make(map[string]error)
	for i, msg := range messages {
		response := resp.Responses[i]
		err := response.Error
		if err == nil && response.AppendResult == nil {
			return merr.WrapErrServiceInternalMsg("append succeeded without commit result")
		}
		if previous, ok := outcomes[msg.VChannel()]; !ok || previous == nil {
			outcomes[msg.VChannel()] = err
		}
	}
	if ts := resp.MaxTimeTick(); ts > s.timestamp {
		s.timestamp = ts
	}
	ids := it.result.GetIDs()
	if typeutil.GetSizeOfIDs(ids) != len(s.writeRows) {
		return merr.WrapErrServiceInternalMsg("partial update destination IDs are not aligned")
	}
	channels, err := it.chMgr.GetVChannels(it.collectionID)
	if err != nil {
		// Append already happened. A transient metadata failure is not a safe
		// admission rejection and must not authorize replaying the write.
		return merr.WrapErrServiceInternalErr(err, "resolve channels for partial update append results")
	}
	indexes, err := it.partialUpdateCASChannelIndexes(ids, channels)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "route partial update append results")
	}
	deletes := make(map[any]struct{}, typeutil.GetSizeOfIDs(it.deletePKs))
	for row := 0; row < typeutil.GetSizeOfIDs(it.deletePKs); row++ {
		deletes[typeutil.GetPK(it.deletePKs, int64(row))] = struct{}{}
	}
	s.retryRows = nil
	for offset, row := range s.writeRows {
		channel := channels[indexes[offset]]
		outcome, ok := outcomes[channel]
		if !ok {
			return merr.WrapErrServiceInternalMsg("missing append result for channel %s", channel)
		}
		original := s.rows[row]
		if outcome == nil {
			s.written[original] = typeutil.GetPK(ids, int64(offset))
			it.observeConditionalOutcome("written", 1)
			s.insertCount++
			if _, deleted := deletes[typeutil.GetPK(ids, int64(offset))]; deleted {
				s.deleteCount++
			}
		} else if isPartialUpdateCASRetry(outcome) {
			s.retryRows = append(s.retryRows, original)
		}
	}
	return nil
}

func (it *UpsertTask) observeConditionalOutcome(outcome string, count int) {
	if count > 0 && it.req.GetWriteCondition() != nil {
		metrics.ProxyUpsertWriteConditionRows.WithLabelValues(paramtable.GetStringNodeID(), it.req.GetDbName(), it.req.GetCollectionName(), outcome).Add(float64(count))
	}
}
