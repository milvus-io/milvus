package antlrparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mysqld/planner"
)

func Test_antlrParser_Parse(t *testing.T) {
	restore := func(t *testing.T, n planner.Node) {
		text := planner.NewExprTextRestorer().RestoreExprText(n)
		fmt.Println(text)
	}

	debug := func(t *testing.T, sql string) {
		plan, warns, err := NewAntlrParser().Parse(sql)
		assert.NoError(t, err)
		assert.Nil(t, warns)
		fmt.Println(sql)
		planner.NewTreeUtils().PrettyPrintHrn(GetSqlStatements(plan.Node))
		fmt.Println()
		statements := GetSqlStatements(plan.Node)
		expr := statements.Statements[0].
			DmlStatement.Unwrap().
			SelectStatement.Unwrap().
			SimpleSelect.Unwrap().
			Query.Unwrap().
			From.Unwrap().
			Where
		if expr.IsSome() {
			restore(t, expr.Unwrap())
		}
	}

	sqls := []string{
		"select a as f1, b as f2 from t",                                        // query without predicate
		"select a as f1, b as f2 from t where a > 5 and b < 2",                  // query with predicate
		"select count(*) as cnt from t",                                         // count without predicate
		"select count(*) as cnt from t where a > 5 and b < 2",                   // count with predicate
		"select a as f1, b as f2 from t where a > 5 and b < 2 limit 9, 4",       // query with predicate and limit
		"select a as f1, b as f2 from t where a > 5 and b < 2 limit 4 offset 9", // query with predicate and limit
	}

	for _, sql := range sqls {
		debug(t, sql)
	}
}
