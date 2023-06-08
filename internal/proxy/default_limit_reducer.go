package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

	if len(result.FieldsData) > 0 {
		result.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}
	} else {
		result.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_EmptyCollection,
			Reason:    "empty collection", // TODO
		}
		return nil
	}

	for i := 0; i < len(result.FieldsData); i++ {
		if outputFieldsID[i] == common.TimeStampField {
			result.FieldsData = append(result.FieldsData[:i], result.FieldsData[(i+1):]...)
			i--
			continue
		}
		for _, field := range schema.Fields {
			if field.FieldID == outputFieldsID[i] {
				// deal with the situation that offset equal to or greater than the number of entities
				if result.FieldsData[i] == nil {
					var err error
					result.FieldsData[i], err = typeutil.GenEmptyFieldData(field)
					if err != nil {
						return err
					}
				}
				result.FieldsData[i].FieldName = field.Name
				result.FieldsData[i].FieldId = field.FieldID
				result.FieldsData[i].Type = field.DataType
				result.FieldsData[i].IsDynamic = field.IsDynamic
			}
		}
	}

	return nil
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
