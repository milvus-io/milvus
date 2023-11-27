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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type InsertBufferSuite struct {
	suite.Suite
	collSchema *schemapb.CollectionSchema
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
}

func (s *InsertBufferSuite) composeInsertMsg(rowCount int, dim int) ([]int64, *msgstream.InsertMsg) {
	tss := lo.RepeatBy(rowCount, func(idx int) int64 { return int64(tsoutil.ComposeTSByTime(time.Now(), int64(idx))) })
	vectors := lo.RepeatBy(rowCount, func(_ int) []float32 {
		return lo.RepeatBy(dim, func(_ int) float32 { return rand.Float32() })
	})
	flatten := lo.Flatten(vectors)
	return tss, &msgstream.InsertMsg{
		InsertRequest: msgpb.InsertRequest{
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
	s.Run("normal_buffer", func() {
		pks, insertMsg := s.composeInsertMsg(10, 128)

		insertBuffer, err := NewInsertBuffer(s.collSchema)
		s.Require().NoError(err)

		fieldData, err := insertBuffer.Buffer([]*msgstream.InsertMsg{insertMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		s.NoError(err)

		pkData := lo.Map(fieldData, func(fd storage.FieldData, _ int) []int64 {
			return lo.RepeatBy(fd.RowNum(), func(idx int) int64 { return fd.GetRow(idx).(int64) })
		})
		s.ElementsMatch(pks, lo.Flatten(pkData))
		s.EqualValues(100, insertBuffer.MinTimestamp())
	})

	s.Run("pk_not_found", func() {
		_, insertMsg := s.composeInsertMsg(10, 128)

		insertMsg.FieldsData = []*schemapb.FieldData{insertMsg.FieldsData[0], insertMsg.FieldsData[1], insertMsg.FieldsData[3]}

		insertBuffer, err := NewInsertBuffer(s.collSchema)
		s.Require().NoError(err)

		_, err = insertBuffer.Buffer([]*msgstream.InsertMsg{insertMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		s.Error(err)
	})

	s.Run("schema_without_pk", func() {
		badSchema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64,
				},
				{
					FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64,
				},
				{
					FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "128"},
					},
				},
			},
		}

		_, insertMsg := s.composeInsertMsg(10, 128)

		insertBuffer, err := NewInsertBuffer(badSchema)
		s.Require().NoError(err)

		_, err = insertBuffer.Buffer([]*msgstream.InsertMsg{insertMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		s.Error(err)
	})
}

func (s *InsertBufferSuite) TestYield() {
	insertBuffer, err := NewInsertBuffer(s.collSchema)
	s.Require().NoError(err)

	result := insertBuffer.Yield()
	s.Nil(result)

	insertBuffer, err = NewInsertBuffer(s.collSchema)
	s.Require().NoError(err)

	pks, insertMsg := s.composeInsertMsg(10, 128)
	_, err = insertBuffer.Buffer([]*msgstream.InsertMsg{insertMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
	s.Require().NoError(err)

	result = insertBuffer.Yield()
	s.NotNil(result)

	pkField, ok := result.Data[common.StartOfUserFieldID]
	s.Require().True(ok)
	pkData := lo.RepeatBy(pkField.RowNum(), func(idx int) int64 { return pkField.GetRow(idx).(int64) })
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
		{
			tag: "missing_type_param",
			schema: &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
					},
					{
						FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
					},
				},
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
