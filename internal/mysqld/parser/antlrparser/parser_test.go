package antlrparser

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/mysqld/parser"
	"github.com/milvus-io/milvus/internal/mysqld/planner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
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

type ANNSSuite struct {
	suite.Suite

	p parser.Parser
}

func (suite *ANNSSuite) SetupTest() {
	suite.p = NewAntlrParser()
}

func (suite *ANNSSuite) TearDownTest() {}

func TestANNSSuite(t *testing.T) {
	suite.Run(t, new(ANNSSuite))
}

func (suite *ANNSSuite) TestFloatVector() {
	sql := `
select query_number, id, distance
from t
where id >= 1000 and id <= 10000
anns by feature -> ([0.23, 0.21], [0.24, 0.26]) PARAMS = (nprobe=1, ef=5)
limit 100
`

	plan, warns, err := suite.p.Parse(sql)
	suite.NoError(err)
	suite.Nil(warns)

	planner.NewTreeUtils().PrettyPrintHrn(GetSqlStatements(plan.Node))
}
