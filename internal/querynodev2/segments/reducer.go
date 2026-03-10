package segments

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type TimestampedRetrieveResult[T interface {
	typeutil.ResultWithID
	GetFieldsData() []*schemapb.FieldData
}] struct {
	Result     T
	Timestamps []int64
}

func (r *TimestampedRetrieveResult[T]) GetIds() *schemapb.IDs {
	return r.Result.GetIds()
}

func (r *TimestampedRetrieveResult[T]) GetHasMoreResult() bool {
	return r.Result.GetHasMoreResult()
}

func (r *TimestampedRetrieveResult[T]) GetTimestamps() []int64 {
	return r.Timestamps
}

func NewTimestampedRetrieveResult[T interface {
	typeutil.ResultWithID
	GetFieldsData() []*schemapb.FieldData
}](result T) (*TimestampedRetrieveResult[T], error) {
	tsField, has := lo.Find(result.GetFieldsData(), func(fd *schemapb.FieldData) bool {
		return fd.GetFieldId() == common.TimeStampField
	})
	if !has {
		return nil, merr.WrapErrServiceInternal("RetrieveResult does not have timestamp field")
	}
	timestamps := tsField.GetScalars().GetLongData().GetData()
	idSize := typeutil.GetSizeOfIDs(result.GetIds())

	if idSize != len(timestamps) {
		return nil, merr.WrapErrServiceInternal("id length is not equal to timestamp length")
	}

	return &TimestampedRetrieveResult[T]{
		Result:     result,
		Timestamps: timestamps,
	}, nil
}
