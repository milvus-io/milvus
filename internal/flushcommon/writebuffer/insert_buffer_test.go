package writebuffer

import (
	"math/rand"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type InsertBufferSuite struct {
	suite.Suite
	collSchema *schemapb.CollectionSchema
	pkField    *schemapb.FieldSchema
}

func (s *InsertBufferSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	s.collSchema = &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64,
			},
			{
				FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64,
			},
			{
				FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
			},
			{
				FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
	s.pkField = &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
}

func (s *InsertBufferSuite) composeInsertMsg(rowCount int, dim int) ([]int64, *msgstream.InsertMsg) {
	tss := lo.RepeatBy(rowCount, func(idx int) int64 { return int64(tsoutil.ComposeTSByTime(time.Now(), int64(idx))) })
	vectors := lo.RepeatBy(rowCount, func(_ int) []float32 {
		return lo.RepeatBy(dim, func(_ int) float32 { return rand.Float32() })
	})
	flatten := lo.Flatten(vectors)
	return tss, &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			Version:    msgpb.InsertDataVersion_ColumnBased,
			RowIDs:     tss,
			Timestamps: lo.Map(tss, func(id int64, _ int) uint64 { return uint64(id) }),
			FieldsData: []*schemapb.FieldData{
				{
					FieldId: common.RowIDField, FieldName: common.RowIDFieldName, Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: tss,
								},
							},
						},
					},
				},
				{
					FieldId: common.TimeStampField, FieldName: common.TimeStampFieldName, Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: tss,
								},
							},
						},
					},
				},
				{
					FieldId: common.StartOfUserFieldID, FieldName: "pk", Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: tss,
								},
							},
						},
					},
				},
				{
					FieldId: common.StartOfUserFieldID + 1, FieldName: "vector", Type: schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: int64(dim),
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{
									Data: flatten,
								},
							},
						},
					},
				},
			},
		},
	}
}

func (s *InsertBufferSuite) TestBasic() {
	s.Run("normal_insertbuffer", func() {
		insertBuffer, err := NewInsertBuffer(s.collSchema)
		s.Require().NoError(err)

		s.True(insertBuffer.IsEmpty())
		s.False(insertBuffer.IsFull())

		insertBuffer.size = insertBuffer.sizeLimit + 1
		s.True(insertBuffer.IsFull())
		s.False(insertBuffer.IsEmpty())
	})
}

func (s *InsertBufferSuite) TestBuffer() {
	_, insertMsg := s.composeInsertMsg(10, 128)

	insertBuffer, err := NewInsertBuffer(s.collSchema)
	s.Require().NoError(err)

	groups, err := PrepareInsert(s.collSchema, s.pkField, []*msgstream.InsertMsg{insertMsg})
	s.Require().NoError(err)
	s.Require().Len(groups, 1)

	memSize := insertBuffer.Buffer(groups[0], &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})

	s.EqualValues(100, insertBuffer.MinTimestamp())
	s.EqualValues(5367, memSize)
}

func (s *InsertBufferSuite) TestYield() {
	insertBuffer, err := NewInsertBuffer(s.collSchema)
	s.Require().NoError(err)

	result := insertBuffer.Yield()
	s.Nil(result)

	insertBuffer, err = NewInsertBuffer(s.collSchema)
	s.Require().NoError(err)

	pks, insertMsg := s.composeInsertMsg(10, 128)
	groups, err := PrepareInsert(s.collSchema, s.pkField, []*msgstream.InsertMsg{insertMsg})
	s.Require().NoError(err)
	s.Require().Len(groups, 1)

	insertBuffer.Buffer(groups[0], &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})

	result = insertBuffer.Yield()
	s.NotNil(result)

	var pkData []int64
	for _, chunk := range result {
		pkField, ok := chunk.Data[common.StartOfUserFieldID]
		s.Require().True(ok)
		pkData = append(pkData, lo.RepeatBy(pkField.RowNum(), func(idx int) int64 { return pkField.GetRow(idx).(int64) })...)
	}
	s.ElementsMatch(pks, pkData)
}

type InsertBufferConstructSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (*InsertBufferConstructSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *InsertBufferConstructSuite) TestCreateSuccess() {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
			},
			{
				FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
	insertBuffer, err := NewInsertBuffer(schema)
	s.NoError(err)
	s.NotNil(insertBuffer)
}

func (s *InsertBufferConstructSuite) TestCreateFailure() {
	type testCase struct {
		schema *schemapb.CollectionSchema
		tag    string
	}

	cases := []testCase{
		{
			tag: "undefined_datatype",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 101, Name: "vector", DataType: -1},
				},
			},
		},
		{
			tag: "mssing_maxlength",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 101, Name: "string", DataType: schemapb.DataType_VarChar},
				},
			},
		},
		{
			tag: "empty_schema",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{},
			},
		},
	}
	for _, tc := range cases {
		s.Run(tc.tag, func() {
			_, err := NewInsertBuffer(tc.schema)
			s.Error(err)
		})
	}
}

func TestInsertBuffer(t *testing.T) {
	suite.Run(t, new(InsertBufferSuite))
	suite.Run(t, new(InsertBufferConstructSuite))
}
