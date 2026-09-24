package dml

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func Test_InsertTaskcheckFieldsDataBySchema(t *testing.T) {
	paramtable.Init()
	mlog.Info(context.TODO(), "InsertTaskcheckFieldsDataBySchema", mlog.Bool("enable", paramtable.Get().ProxyCfg.SkipAutoIDCheck.GetAsBool()))
	var err error

	t.Run("schema is empty, though won't happen in system", func(t *testing.T) {
		// won't happen in system
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields:      []*schemapb.FieldSchema{},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					DbName:         "TestInsertTask_checkFieldsDataBySchema",
					CollectionName: "TestInsertTask_checkFieldsDataBySchema",
					PartitionName:  "TestInsertTask_checkFieldsDataBySchema",
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 0)
	})

	t.Run("miss field", func(t *testing.T) {
		// schema has field, msg has no field.
		// schema is not Nullable or has set default_value
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:     "a",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("miss field is nullable or set default_value", func(t *testing.T) {
		// schema has fields, msg has no field.
		// schema is Nullable or set default_value
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,

				Fields: []*schemapb.FieldSchema{
					{
						Name:     "a",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
						Nullable: true,
					},
					{
						Name:     "b",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
						DefaultValue: &schemapb.ValueField{
							Data: &schemapb.ValueField_LongData{
								LongData: 1,
							},
						},
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 2)
	})

	t.Run("schema has autoid pk", func(t *testing.T) {
		// schema has autoid pk
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 0)
	})

	t.Run("schema pk is not autoid, but not pass pk", func(t *testing.T) {
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       false,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("pass more data field", func(t *testing.T) {
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "c",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("duplicate field datas", func(t *testing.T) {
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("not pk field, but autoid == true", func(t *testing.T) {
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       true,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("has more than one pk", func(t *testing.T) {
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("pk can not set default value", func(t *testing.T) {
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       false,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
						DefaultValue: &schemapb.ValueField{
							Data: &schemapb.ValueField_LongData{
								LongData: 1,
							},
						},
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, false)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})
	t.Run("normal when upsert", func(t *testing.T) {
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "Test_CheckFieldsDataBySchema",
				Description: "Test_CheckFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       false,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       false,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, false)
		assert.NoError(t, err)

		task = InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "Test_CheckFieldsDataBySchema",
				Description: "Test_CheckFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       false,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}
		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, false)
		assert.NoError(t, err)
	})

	t.Run("skip the auto id", func(t *testing.T) {
		task := InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_fillFieldsDataBySchema",
				Description: "TestInsertTask_fillFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:     "b",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 2)

		paramtable.Get().Save(paramtable.Get().ProxyCfg.SkipAutoIDCheck.Key, "true")
		task = InsertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_fillFieldsDataBySchema",
				Description: "TestInsertTask_fillFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:     "b",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(context.TODO(), task.schema.Fields, task.schema, task.insertMsg, true)
		assert.NoError(t, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 2)
		paramtable.Get().Reset(paramtable.Get().ProxyCfg.SkipAutoIDCheck.Key)
	})
}

func Test_InsertTaskCheckPrimaryFieldData(t *testing.T) {
	// schema is empty, though won't happen in system
	// num_rows(0) should be greater than 0
	case1 := InsertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				DbName:         "TestInsertTask_checkPrimaryFieldData",
				CollectionName: "TestInsertTask_checkPrimaryFieldData",
				PartitionName:  "TestInsertTask_checkPrimaryFieldData",
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}

	_, err := checkPrimaryFieldData(context.TODO(), case1.schema.Fields, case1.schema, case1.insertMsg)
	assert.NotEqual(t, nil, err)

	// the num of passed fields is less than needed
	case2 := InsertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				RowData: []*commonpb.Blob{
					{},
					{},
				},
				FieldsData: []*schemapb.FieldData{
					{
						Type: schemapb.DataType_Int64,
					},
				},
				Version: msgpb.InsertDataVersion_RowBased,
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}
	_, err = checkPrimaryFieldData(context.TODO(), case2.schema.Fields, case2.schema, case2.insertMsg)
	assert.NotEqual(t, nil, err)

	// autoID == false, no primary field schema
	// primary field is not found
	case3 := InsertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "int64Field",
					DataType: schemapb.DataType_Int64,
				},
				{
					Name:     "floatField",
					DataType: schemapb.DataType_Float,
				},
			},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				RowData: []*commonpb.Blob{
					{},
					{},
				},
				FieldsData: []*schemapb.FieldData{
					{},
					{},
				},
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}
	_, err = checkPrimaryFieldData(context.TODO(), case3.schema.Fields, case3.schema, case3.insertMsg)
	assert.NotEqual(t, nil, err)

	// autoID == true, has primary field schema, but primary field data exist
	// can not assign primary field data when auto id enabled int64Field
	case4 := InsertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "int64Field",
					FieldID:  1,
					DataType: schemapb.DataType_Int64,
				},
				{
					Name:     "floatField",
					FieldID:  2,
					DataType: schemapb.DataType_Float,
				},
			},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				RowData: []*commonpb.Blob{
					{},
					{},
				},
				FieldsData: []*schemapb.FieldData{
					{
						Type:      schemapb.DataType_Int64,
						FieldName: "int64Field",
					},
				},
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}
	case4.schema.Fields[0].IsPrimaryKey = true
	case4.schema.Fields[0].AutoID = true
	case4.insertMsg.FieldsData[0] = newScalarFieldData(case4.schema.Fields[0], case4.schema.Fields[0].Name, 10)
	_, err = checkPrimaryFieldData(context.TODO(), case4.schema.Fields, case4.schema, case4.insertMsg)
	assert.NotEqual(t, nil, err)

	// autoID == true, has primary field schema, but DataType don't match
	// the data type of the data not matches the schema
	case4.schema.Fields[0].IsPrimaryKey = false
	case4.schema.Fields[1].IsPrimaryKey = true
	case4.schema.Fields[1].AutoID = true
	_, err = checkPrimaryFieldData(context.TODO(), case4.schema.Fields, case4.schema, case4.insertMsg)
	assert.NotEqual(t, nil, err)
}

func Test_UpsertTaskCheckPrimaryFieldData(t *testing.T) {
	for _, autoID := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			numRows  uint64
			field    *schemapb.FieldData
			nullable bool
			noPK     bool
			valid    bool
		}{
			{name: "preserve without non-PK fields", numRows: 1, field: partialUpdateCASPKFieldData([]int64{10}), valid: true},
			{name: "zero rows", field: partialUpdateCASPKFieldData([]int64{10})},
			{name: "no primary key schema", numRows: 1, noPK: true},
			{name: "nullable primary key", numRows: 1, nullable: true},
			{name: "missing primary key", numRows: 1},
			{name: "missing scalar payload", numRows: 1, field: &schemapb.FieldData{FieldName: "id", Type: schemapb.DataType_Int64}},
			{name: "empty primary key", numRows: 1, field: partialUpdateCASPKFieldData([]int64{})},
			{name: "row count mismatch", numRows: 2, field: partialUpdateCASPKFieldData([]int64{10})},
			{name: "wrong primary key type", numRows: 1, field: partialUpdateCASStringPKFieldData([]string{"10"})},
		} {
			t.Run(fmt.Sprintf("autoID=%t/%s", autoID, tc.name), func(t *testing.T) {
				collection := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: !tc.noPK, AutoID: autoID, Nullable: tc.nullable},
					{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
				}}
				helper, err := typeutil.CreateSchemaHelper(collection)
				require.NoError(t, err)
				schema := &schemaInfo{CollectionSchema: collection, SchemaHelper: helper}
				var fields []*schemapb.FieldData
				if tc.field != nil {
					fields = []*schemapb.FieldData{tc.field}
				}

				ids, err := checkUpsertPrimaryFieldData(schema, fields, tc.numRows, nil)
				if !tc.valid {
					require.Error(t, err)
					require.Nil(t, ids)
					return
				}
				require.NoError(t, err)
				require.Equal(t, []int64{10}, ids.GetIntId().GetData())
				require.Len(t, fields, 1, "PK validation must not fill omitted non-PK fields")
				require.Same(t, tc.field, fields[0])
				require.Equal(t, []int64{10}, fields[0].GetScalars().GetLongData().GetData())
			})
		}
	}
}
