package proxy

import (
	"context"

	"go.uber.org/zap"
	
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type defaultLimitReducer struct {
	ctx            context.Context
	req            *internalpb.RetrieveRequest
	params         *queryParams
	schema         *schemapb.CollectionSchema
	collectionName string
}

func (r *defaultLimitReducer) Reduce(results []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	res, err := reduceRetrieveResultsAndFillIfEmpty(r.ctx, results, r.params, r.req.GetOutputFieldsId(), r.schema)
	if err != nil {
		return nil, err
	}

	if err := r.afterReduce(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (r *defaultLimitReducer) afterReduce(result *milvuspb.QueryResults) error {
	collectionName := r.collectionName
	schema := r.schema
	outputFieldsID := r.req.GetOutputFieldsId()

	result.CollectionName = collectionName
	var err error

	for i := 0; i < len(result.GetFieldsData()); i++ {
		fieldID := result.FieldsData[i].GetFieldId()
		
		// 关键修复：跳过负数字段ID（如距离字段 -100），这些是特殊字段不需要处理
		if fieldID < 0 {
			continue
		}
		
		// 确保数组索引安全：如果超出outputFieldsID范围，跳过处理
		if i >= len(outputFieldsID) {
			log.Warn("Field index exceeds outputFieldsID length, skipping",
				zap.Int("fieldIndex", i),
				zap.Int("outputFieldsIDLen", len(outputFieldsID)),
				zap.Int64("fieldID", fieldID))
			continue
		}
		
		// drop ts column
		if outputFieldsID[i] == common.TimeStampField {
			result.FieldsData = append(result.FieldsData[:i], result.FieldsData[(i+1):]...)
			outputFieldsID = append(outputFieldsID[:i], outputFieldsID[i+1:]...)
			i--
			continue
		}
		field := typeutil.GetField(schema, outputFieldsID[i])
		if field == nil {
			err = merr.WrapErrFieldNotFound(outputFieldsID[i])
			break
		}

		if result.FieldsData[i] == nil {
			result.FieldsData[i], err = typeutil.GenEmptyFieldData(field)
			if err != nil {
				break
			}
			continue
		}

		result.FieldsData[i].FieldName = field.GetName()
		result.FieldsData[i].FieldId = field.GetFieldID()
		result.FieldsData[i].Type = field.GetDataType()
		result.FieldsData[i].IsDynamic = field.GetIsDynamic()
	}

	result.Status = merr.Status(err)
	return err
}

func newDefaultLimitReducer(ctx context.Context, params *queryParams, req *internalpb.RetrieveRequest, schema *schemapb.CollectionSchema, collectionName string) *defaultLimitReducer {
	return &defaultLimitReducer{
		ctx:            ctx,
		req:            req,
		params:         params,
		schema:         schema,
		collectionName: collectionName,
	}
}
