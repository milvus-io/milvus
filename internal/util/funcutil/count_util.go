package funcutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
)

func CntOfInternalResult(res *internalpb.RetrieveResults) (int64, error) {
	if len(res.GetFieldsData()) != 1 {
		return 0, fmt.Errorf("internal count result should only have one column")
	}

	f := res.GetFieldsData()[0]
	return CntOfFieldData(f)
}

func CntOfSegCoreResult(res *segcorepb.RetrieveResults) (int64, error) {
	if len(res.GetFieldsData()) != 1 {
		return 0, fmt.Errorf("segcore count result should only have one column")
	}

	f := res.GetFieldsData()[0]
	return CntOfFieldData(f)
}

func CntOfFieldData(f *schemapb.FieldData) (int64, error) {
	scalars := f.GetScalars()
	if scalars == nil {
		return 0, fmt.Errorf("count result should be scalar")
	}
	data := scalars.GetLongData()
	if data == nil {
		return 0, fmt.Errorf("count result should be int64 data")
	}
	if len(data.GetData()) != 1 {
		return 0, fmt.Errorf("count result shoud only have one row")
	}
	return data.GetData()[0], nil
}

func WrapCntToInternalResult(cnt int64) *internalpb.RetrieveResults {
	return &internalpb.RetrieveResults{
		Status:     &commonpb.Status{},
		FieldsData: []*schemapb.FieldData{WrapCntToFieldData(cnt)},
	}
}

func WrapCntToSegCoreResult(cnt int64) *segcorepb.RetrieveResults {
	return &segcorepb.RetrieveResults{
		Ids:        nil,
		Offset:     nil,
		FieldsData: []*schemapb.FieldData{WrapCntToFieldData(cnt)},
	}
}

func WrapCntToFieldData(cnt int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "count(*)",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{cnt},
					},
				},
			},
		},
		FieldId: 0,
	}
}

func WrapCntToQueryResults(cnt int64) *milvuspb.QueryResults {
	return &milvuspb.QueryResults{
		Status:         &commonpb.Status{},
		FieldsData:     []*schemapb.FieldData{WrapCntToFieldData(cnt)},
		CollectionName: "",
	}
}

func CntOfQueryResults(res *milvuspb.QueryResults) (int64, error) {
	if len(res.GetFieldsData()) != 1 {
		return 0, fmt.Errorf("milvus count result should only have one column")
	}

	f := res.GetFieldsData()[0]
	return CntOfFieldData(f)
}
