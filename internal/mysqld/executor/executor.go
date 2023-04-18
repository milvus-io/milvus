package executor

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/pkg/common"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/mysqld/parser/antlrparser"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"

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

	if q.Anns.IsSome() {
		// reuse the parsed `outputFields`.
		return e.execANNS(ctx, q, outputFields)
	}

	if q.Limit.IsSome() {
		// TODO: use pagination.
		return nil, fmt.Errorf("invalid query statement, limit/offset is not supported")
	}

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

func (e *defaultExecutor) execANNS(ctx context.Context, q *planner.NodeQuerySpecification, outputs []string) (*sqltypes.Result, error) {
	if !q.Limit.IsSome() {
		return nil, fmt.Errorf("limit not specified in the ANNS statement")
	}

	annsClause := q.Anns.Unwrap()

	searchParams := prepareSearchParams(q.Limit.Unwrap(), annsClause)

	outputsIndex, userOutputs := generateOutputsIndex(outputs)

	filter := restoreExpr(q.From.Unwrap())

	tableName := q.From.Unwrap().TableSources[0].TableName.Unwrap()

	req := prepareSearchReq(tableName, filter, annsClause.Vectors, userOutputs, searchParams)

	res, err := e.s.Search(ctx, req)
	if err != nil {
		return nil, err
	}

	if res.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, common.NewStatusError(res.GetStatus().GetErrorCode(), res.GetStatus().GetReason())
	}

	return wrapSearchResult(res, outputsIndex, userOutputs), nil
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

func NewDefaultExecutor(s types.ProxyComponent) Executor {
	return &defaultExecutor{s: s}
}
