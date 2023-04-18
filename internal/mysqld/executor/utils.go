package executor

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"

	"github.com/milvus-io/milvus/internal/mysqld/planner"
	"github.com/milvus-io/milvus/pkg/common"
)

func prepareSearchParams(limitClause *planner.NodeLimitClause, annsClause *planner.NodeANNSClause) map[string]string {
	searchParams := map[string]string{
		common.AnnsFieldKey: annsClause.Column.Name,
		common.TopKKey:      fmt.Sprintf("%d", limitClause.Limit),
		common.OffsetKey:    fmt.Sprintf("%d", limitClause.Offset),
	}

	if annsClause.Params.IsSome() {
		for k, v := range annsClause.Params.Unwrap().KVs {
			searchParams[k] = v
		}
		params, _ := json.Marshal(annsClause.Params.Unwrap().KVs)
		searchParams[common.SearchParamsKey] = string(params)
	}

	return searchParams
}

func generateOutputsIndex(outputs []string) (index map[string]int, users []string) {
	index = make(map[string]int, len(outputs))
	users = make([]string, 0, len(outputs))

	fix := func(s string) string {
		return strings.ToLower(strings.TrimSpace(s))
	}

	for i, f := range outputs {
		fixed := fix(f)
		if fixed != common.QueryNumberKey && fixed != common.DistanceKey {
			users = append(users, f)
			index[f] = i
		} else {
			index[fixed] = i
		}
	}

	return index, users
}

func restoreExpr(from *planner.NodeFromClause) string {
	filter := ""
	if from.Where.IsSome() {
		filter = planner.NewExprTextRestorer().RestoreExprText(from.Where.Unwrap())
	}
	return filter
}

func vector2PlaceholderGroupBytes(vectors []*planner.NodeVector) []byte {
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			vector2Placeholder(vectors),
		},
	}

	bs, _ := proto.Marshal(phg)
	return bs
}

func vector2Placeholder(vectors []*planner.NodeVector) *commonpb.PlaceholderValue {
	var placeHolderType commonpb.PlaceholderType

	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Values: make([][]byte, 0, len(vectors)),
	}

	if len(vectors) == 0 {
		return ph
	}

	if vectors[0].FloatVector.IsSome() {
		placeHolderType = commonpb.PlaceholderType_FloatVector
	} else {
		placeHolderType = commonpb.PlaceholderType_BinaryVector

	}

	ph.Type = placeHolderType
	for _, vector := range vectors {
		ph.Values = append(ph.Values, vector.Serialize())
	}

	return ph
}

func prepareSearchReq(tableName string, filter string, vectors []*planner.NodeVector, userOutputs []string, searchParams map[string]string) *milvuspb.SearchRequest {
	phg := vector2PlaceholderGroupBytes(vectors)
	req := &milvuspb.SearchRequest{
		CollectionName:     tableName,
		PartitionNames:     nil,
		Dsl:                filter,
		PlaceholderGroup:   phg,
		DslType:            commonpb.DslType_BoolExprV1,
		OutputFields:       userOutputs,
		SearchParams:       funcutil.Map2KeyValuePair(searchParams),
		TravelTimestamp:    0,
		GuaranteeTimestamp: 2, // default bounded consistency level.
		Nq:                 int64(len(vectors)),
	}
	return req
}

func wrapFieldsData(collectionName string, fieldsData []*schemapb.FieldData) *sqltypes.Result {
	nColumn := len(fieldsData)
	fields := make([]*querypb.Field, 0, nColumn)

	if nColumn <= 0 {
		return &sqltypes.Result{}
	}

	for i := 0; i < nColumn; i++ {
		fields = append(fields, getSQLField(collectionName, fieldsData[i]))
	}

	nRow := typeutil.GetRowCount(fieldsData[0])
	rows := make([][]sqltypes.Value, 0, nRow)
	for i := 0; i < nRow; i++ {
		row := make([]sqltypes.Value, 0, nColumn)
		for j := 0; j < nColumn; j++ {
			row = append(row, getDataSingle(fieldsData[j], i))
		}
		rows = append(rows, row)
	}

	return &sqltypes.Result{
		Fields: fields,
		Rows:   rows,
	}
}

func wrapQueryResults(res *milvuspb.QueryResults) *sqltypes.Result {
	return wrapFieldsData(res.GetCollectionName(), res.GetFieldsData())
}

func wrapCountResult(rowCnt int, column string) *sqltypes.Result {
	result1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: column,
				Type: querypb.Type_INT64,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt64(int64(rowCnt)),
			},
		},
	}
	return result1
}

func getSQLField(tableName string, fieldData *schemapb.FieldData) *querypb.Field {
	return &querypb.Field{
		Name:         fieldData.GetFieldName(),
		Type:         toSQLType(fieldData.GetType()),
		Table:        tableName,
		OrgTable:     "",
		Database:     "",
		OrgName:      "",
		ColumnLength: 0,
		Charset:      0,
		Decimals:     0,
		Flags:        0,
	}
}

func toSQLType(t schemapb.DataType) querypb.Type {
	switch t {
	case schemapb.DataType_Bool:
		// TODO: tinyint
		return querypb.Type_UINT8
	case schemapb.DataType_Int8:
		return querypb.Type_INT8
	case schemapb.DataType_Int16:
		return querypb.Type_INT16
	case schemapb.DataType_Int32:
		return querypb.Type_INT32
	case schemapb.DataType_Int64:
		return querypb.Type_INT64
	case schemapb.DataType_Float:
		return querypb.Type_FLOAT32
	case schemapb.DataType_Double:
		return querypb.Type_FLOAT64
	case schemapb.DataType_VarChar:
		return querypb.Type_VARCHAR
		// TODO: vector.
	default:
		return querypb.Type_NULL_TYPE
	}
}

func getDataSingle(fieldData *schemapb.FieldData, idx int) sqltypes.Value {
	switch fieldData.GetType() {
	case schemapb.DataType_Bool:
		// TODO: tinyint
		return sqltypes.NewInt32(1)
	case schemapb.DataType_Int8:
		v := fieldData.Field.(*schemapb.FieldData_Scalars).Scalars.Data.(*schemapb.ScalarField_IntData).IntData.GetData()[idx]
		return sqltypes.MakeTrusted(sqltypes.Int8, strconv.AppendInt(nil, int64(v), 10))
	case schemapb.DataType_Int16:
		v := fieldData.Field.(*schemapb.FieldData_Scalars).Scalars.Data.(*schemapb.ScalarField_IntData).IntData.GetData()[idx]
		return sqltypes.MakeTrusted(sqltypes.Int16, strconv.AppendInt(nil, int64(v), 10))
	case schemapb.DataType_Int32:
		v := fieldData.Field.(*schemapb.FieldData_Scalars).Scalars.Data.(*schemapb.ScalarField_IntData).IntData.GetData()[idx]
		return sqltypes.MakeTrusted(sqltypes.Int32, strconv.AppendInt(nil, int64(v), 10))
	case schemapb.DataType_Int64:
		v := fieldData.Field.(*schemapb.FieldData_Scalars).Scalars.Data.(*schemapb.ScalarField_LongData).LongData.GetData()[idx]
		return sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, v, 10))
	case schemapb.DataType_Float:
		v := fieldData.Field.(*schemapb.FieldData_Scalars).Scalars.Data.(*schemapb.ScalarField_FloatData).FloatData.GetData()[idx]
		return sqltypes.MakeTrusted(sqltypes.Float32, strconv.AppendFloat(nil, float64(v), 'f', -1, 64))
	case schemapb.DataType_Double:
		v := fieldData.Field.(*schemapb.FieldData_Scalars).Scalars.Data.(*schemapb.ScalarField_DoubleData).DoubleData.GetData()[idx]
		return sqltypes.MakeTrusted(sqltypes.Float64, strconv.AppendFloat(nil, v, 'g', -1, 64))
	case schemapb.DataType_VarChar:
		v := fieldData.Field.(*schemapb.FieldData_Scalars).Scalars.Data.(*schemapb.ScalarField_StringData).StringData.GetData()[idx]
		return sqltypes.NewVarChar(v)

		// TODO: vector.
	default:
		// TODO: should raise error here.
		return sqltypes.NewInt32(1)
	}
}

func wrapSearchResult(res *milvuspb.SearchResults, outputsIndex map[string]int, userOutputs []string) *sqltypes.Result {
	qnIndex, qnOk := outputsIndex[common.QueryNumberKey]
	disIndex, disOk := outputsIndex[common.DistanceKey]

	fieldsData := make([]*schemapb.FieldData, len(res.GetResults().GetFieldsData()))
	copy(fieldsData, res.GetResults().GetFieldsData())

	insert := func(fields []*schemapb.FieldData, field *schemapb.FieldData, index int) []*schemapb.FieldData {
		if index >= len(fields) {
			return append(fields, field)
		}
		return append(fieldsData[:index], append([]*schemapb.FieldData{field}, fieldsData[index:]...)...)
	}

	if qnOk && disOk {
		qnField := generateQueryNumberFieldData(res)
		disField := generateDistanceFieldsData(res)

		index1, index2 := qnIndex, disIndex
		f1, f2 := qnField, disField
		if qnIndex > disIndex {
			index1, index2 = disIndex, qnIndex
			f1, f2 = disField, qnField
		}

		fieldsData = insert(fieldsData, f1, index1)
		fieldsData = insert(fieldsData, f2, index2)
	} else if qnOk {
		qnField := generateQueryNumberFieldData(res)
		fieldsData = insert(fieldsData, qnField, qnIndex)
	} else if disOk {
		disField := generateDistanceFieldsData(res)
		fieldsData = insert(fieldsData, disField, disIndex)
	}

	return wrapFieldsData(res.GetCollectionName(), fieldsData)
}

func generateQueryNumberFieldData(res *milvuspb.SearchResults) *schemapb.FieldData {
	arr := make([]int64, 0, res.GetResults().GetNumQueries()*res.GetResults().GetTopK())

	for i, topk := range res.GetResults().GetTopks() {
		for j := 0; int64(j) < topk; j++ {
			arr = append(arr, int64(i))
		}
	}

	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: common.QueryNumberKey,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: arr,
					},
				},
			},
		},
	}
}

func generateDistanceFieldsData(res *milvuspb.SearchResults) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: common.DistanceKey,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: res.GetResults().GetScores(),
					},
				},
			},
		},
	}
}

func getOutputFieldsOrMatchCountRule(fields []*planner.NodeSelectElement) (outputFields []string, match bool, err error) {
	match = false
	l := len(fields)

	if l == 1 {
		entry := fields[0]
		match = entry.FunctionCall.IsSome() &&
			entry.FunctionCall.Unwrap().Agg.IsSome() &&
			entry.FunctionCall.Unwrap().Agg.Unwrap().AggCount.IsSome()
	}

	if match {
		return nil, match, nil
	}

	outputFields = make([]string, 0, l)
	for _, entry := range fields {
		if entry.Star.IsSome() {
			// TODO: support `select *`.
			return nil, match, fmt.Errorf("* is not supported")
		}

		if entry.FunctionCall.IsSome() {
			return nil, match, fmt.Errorf("combined select elements is not supported")
		}

		if entry.FullColumnName.IsSome() {
			c := entry.FullColumnName.Unwrap()
			if c.Alias.IsSome() {
				return nil, match, fmt.Errorf("alias for select elements is not supported")
			}
			outputFields = append(outputFields, c.Name)
		}
	}

	return outputFields, false, nil
}
