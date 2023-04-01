package executor

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/mysqld/parser/antlrparser"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"

	"github.com/milvus-io/milvus/internal/mysqld/planner"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

type Executor interface {
	Run(ctx context.Context, plan *planner.PhysicalPlan) (*sqltypes.Result, error)
}

// defaultExecutor only translates sql to rpc. TODO: Better to use vacalno model or batch model.
type defaultExecutor struct {
	s types.ProxyComponent
}

func (e *defaultExecutor) Run(ctx context.Context, plan *planner.PhysicalPlan) (*sqltypes.Result, error) {
	statements := antlrparser.GetSqlStatements(plan.Node)
	if statements == nil {
		return nil, fmt.Errorf("invalid node, sql should be parsed to statements")
	}
	l := len(statements.Statements)
	if l != 1 {
		return nil, fmt.Errorf("only one statement is supported")
	}
	return e.dispatch(ctx, statements.Statements[0])
}

func (e *defaultExecutor) dispatch(ctx context.Context, n *planner.NodeSqlStatement) (*sqltypes.Result, error) {
	if n.DmlStatement.IsSome() {
		return e.dispatchDmlStatement(ctx, n.DmlStatement.Unwrap())
	}
	return nil, fmt.Errorf("invalid sql statement, only dml statement is supported")
}

func (e *defaultExecutor) dispatchDmlStatement(ctx context.Context, n *planner.NodeDmlStatement) (*sqltypes.Result, error) {
	if n.SelectStatement.IsSome() {
		return e.execSelect(ctx, n.SelectStatement.Unwrap())
	}
	return nil, fmt.Errorf("invalid dml statement, only select statement is supported")
}

func (e *defaultExecutor) execSelect(ctx context.Context, n *planner.NodeSelectStatement) (*sqltypes.Result, error) {
	if !n.SimpleSelect.IsSome() {
		return nil, fmt.Errorf("invalid select statement, only simple select is supported")
	}

	stmt := n.SimpleSelect.Unwrap()

	if stmt.LockClause.IsSome() {
		return nil, fmt.Errorf("invalid simple select statement, lock clause is not supported")
	}

	if !stmt.Query.IsSome() {
		return nil, fmt.Errorf("invalid simple select statement, only query is supported")
	}

	q := stmt.Query.Unwrap()

	if q.Limit.IsSome() {
		// TODO: use pagination.
		return nil, fmt.Errorf("invalid query statement, limit/offset is not supported")
	}

	if len(q.SelectSpecs) != 0 {
		return nil, fmt.Errorf("invalid query statement, select spec is not supported")
	}

	if !q.From.IsSome() {
		return nil, fmt.Errorf("invalid query statement, table source is not specified")
	}

	from := q.From.Unwrap()
	if len(from.TableSources) != 1 {
		return nil, fmt.Errorf("invalid query statement, only one table source is supported")
	}

	tableName := from.TableSources[0].TableName.Unwrap()

	outputFields, match, err := getOutputFieldsOrMatchCountRule(q.SelectElements)
	if err != nil {
		return nil, err
	}

	if match && !from.Where.IsSome() { // count without filter.
		rowCnt, err := e.execCountWithoutFilter(ctx, tableName)
		if err != nil {
			return nil, err
		}

		result1 := wrapCountResult(rowCnt, "count(*)")
		return result1, nil
	}

	if match && from.Where.IsSome() { // count with filter.
		filter := planner.NewExprTextRestorer().RestoreExprText(from.Where.Unwrap())
		return e.execCountWithFilter(ctx, tableName, filter)
	}

	// `match` is false.

	if !from.Where.IsSome() { // query without filter.
		return nil, fmt.Errorf("query without filter is not supported")
	}

	filter := planner.NewExprTextRestorer().RestoreExprText(from.Where.Unwrap())
	res, err := e.execQuery(ctx, tableName, filter, outputFields)
	if err != nil {
		return nil, err
	}
	return wrapQueryResults(res), nil
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

func (e *defaultExecutor) execCountWithFilter(ctx context.Context, tableName string, filter string) (*sqltypes.Result, error) {
	// TODO: check if `*` match vector field.
	outputs := []string{"*"}

	res, err := e.execQuery(ctx, tableName, filter, outputs)
	if err != nil {
		return nil, err
	}

	nColumn := len(res.GetFieldsData())
	nRow := 0
	if nColumn > 0 {
		nRow = typeutil.GetRowCount(res.GetFieldsData()[0])
	}

	return wrapCountResult(nRow, "count(*)"), nil
}

func (e *defaultExecutor) execCountWithoutFilter(ctx context.Context, tableName string) (int, error) {
	req := &milvuspb.GetCollectionStatisticsRequest{
		Base:           commonpbutil.NewMsgBase(),
		CollectionName: tableName,
	}
	resp, err := e.s.GetCollectionStatistics(ctx, req)
	if err != nil {
		return 0, err
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return 0, errors.New(resp.GetStatus().GetReason())
	}
	rowCnt, err := strconv.Atoi(resp.GetStats()[0].GetValue())
	if err != nil {
		return 0, err
	}
	return rowCnt, nil
}

func (e *defaultExecutor) execQuery(ctx context.Context, tableName string, filter string, outputs []string) (*milvuspb.QueryResults, error) {
	req := &milvuspb.QueryRequest{
		Base:               commonpbutil.NewMsgBase(),
		DbName:             "",
		CollectionName:     tableName,
		Expr:               filter,
		OutputFields:       outputs,
		PartitionNames:     nil,
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
		QueryParams:        nil,
	}

	resp, err := e.s.Query(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, errors.New(resp.GetStatus().GetReason())
	}

	return resp, nil
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

func wrapQueryResults(res *milvuspb.QueryResults) *sqltypes.Result {
	fieldsData := res.GetFieldsData()
	nColumn := len(fieldsData)
	fields := make([]*querypb.Field, 0, nColumn)

	if nColumn <= 0 {
		return &sqltypes.Result{}
	}

	for i := 0; i < nColumn; i++ {
		fields = append(fields, getSQLField(res.GetCollectionName(), fieldsData[i]))
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

func NewDefaultExecutor(s types.ProxyComponent) Executor {
	return &defaultExecutor{s: s}
}
