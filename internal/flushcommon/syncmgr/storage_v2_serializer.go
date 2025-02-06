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

package syncmgr

import (
	"context"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	iTypeutil "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type storageV2Serializer struct {
	*storageV1Serializer

	arrowSchema *arrow.Schema
	metacache   metacache.MetaCache
}

func NewStorageV2Serializer(
	metacache metacache.MetaCache,
) (*storageV2Serializer, error) {
	v1Serializer, err := NewStorageSerializer(metacache)
	if err != nil {
		return nil, err
	}
	arrowSchema, err := iTypeutil.ConvertToArrowSchema(v1Serializer.schema.Fields)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("convert to arrow schema error: %s", err.Error())
	}
	return &storageV2Serializer{
		storageV1Serializer: v1Serializer,
		arrowSchema:         arrowSchema,
		metacache:           metacache,
	}, nil
}

func (s *storageV2Serializer) serializeBinlog(ctx context.Context, pack *SyncPack) (arrow.Record, error) {
	if len(pack.insertData) == 0 {
		return nil, nil
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, s.arrowSchema)
	defer builder.Release()

	for _, chunk := range pack.insertData {
		if err := iTypeutil.BuildRecord(builder, chunk, s.schema.GetFields()); err != nil {
			return nil, err
		}
	}

	rec := builder.NewRecord()
	return rec, nil
}

func (s *storageV2Serializer) serializeDeltalog(ctx context.Context, pack *SyncPack) (arrow.Record, error) {
	if len(pack.deltaData.Pks) == 0 {
		return nil, nil
	}

	if len(pack.deltaData.Pks) != len(pack.deltaData.Tss) {
		return nil, merr.WrapErrParameterInvalidMsg("pk and ts should have same length in delta log, but get %d and %d", len(pack.deltaData.Pks), len(pack.deltaData.Tss))
	}

	fields := make([]*schemapb.FieldSchema, 0, 2)
	tsField := &schemapb.FieldSchema{
		FieldID:  common.TimeStampField,
		Name:     common.TimeStampFieldName,
		DataType: schemapb.DataType_Int64,
	}
	fields = append(fields, s.pkField, tsField)

	deltaArrowSchema, err := iTypeutil.ConvertToArrowSchema(fields)
	if err != nil {
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, deltaArrowSchema)
	defer builder.Release()

	switch s.pkField.GetDataType() {
	case schemapb.DataType_Int64:
		pb := builder.Field(0).(*array.Int64Builder)
		for _, pk := range pack.deltaData.Pks {
			pb.Append(pk.GetValue().(int64))
		}
	case schemapb.DataType_VarChar:
		pb := builder.Field(0).(*array.StringBuilder)
		for _, pk := range pack.deltaData.Pks {
			pb.Append(pk.GetValue().(string))
		}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unexpected pk type %v", s.pkField.GetDataType())
	}

	for _, ts := range pack.deltaData.Tss {
		builder.Field(1).(*array.Int64Builder).Append(int64(ts))
	}

	rec := builder.NewRecord()
	return rec, nil
}
