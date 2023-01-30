package mysqld

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

type handler struct {
	s types.ProxyComponent
}

func (h *handler) ServerVersion() string {
	return "Milvus"
}

func (h *handler) SetServerVersion() {
}

func (h *handler) NewSession(session *driver.Session) {
}

func (h *handler) SessionInc(session *driver.Session) {
}

func (h *handler) SessionDec(session *driver.Session) {
}

func (h *handler) SessionClosed(session *driver.Session) {
}

func (h *handler) SessionCheck(session *driver.Session) error {
	return nil
}

func (h *handler) AuthCheck(session *driver.Session) error {
	return nil
}

func (h *handler) ComInitDB(session *driver.Session, database string) error {
	return nil
}

// ComQuery TODO: investigate how bindVariables work.
func (h *handler) ComQuery(session *driver.Session, query string, bindVariables map[string]*query.BindVariable, callback func(*sqltypes.Result) error) error {
	if err := h.handleSpecialQuery(session, query, bindVariables, callback); err == nil {
		return nil
	}

	f := newDefaultFactory(h.s)

	parser := f.NewParser()
	logicalPlan, warns, err := parser.Parse(query)
	if err != nil {
		return err
	}
	logWarns(warns)

	rbo, cbo := f.NewOptimizer()
	optimizedLogicalPlan := rbo.Optimize(logicalPlan)

	compiler := f.NewCompiler()
	physicalPlan, err := compiler.Compile(optimizedLogicalPlan)
	if err != nil {
		return err
	}

	optimizedPhysicalPlan := cbo.Optimize(physicalPlan)

	e := f.NewExecutor()
	res, err := e.Run(optimizedPhysicalPlan)
	if err != nil {
		return err
	}
	return callback(res)
}

func (h *handler) handleSpecialQuery(session *driver.Session, query string, bindVariables map[string]*query.BindVariable, callback func(*sqltypes.Result) error) error {
	words := strings.Fields(query)

	// select @@version_comment limit 1
	if len(words) == 4 &&
		strings.ToLower(words[0]) == "select" &&
		strings.ToLower(words[1]) == "@@version_comment" &&
		strings.ToLower(words[2]) == "limit" &&
		strings.ToLower(words[3]) == "1" {
		res := &sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "@@version_comment",
					Type: querypb.Type_VARCHAR,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.NewVarChar("TODO"),
				},
			},
		}
		return callback(res)
	}

	return fmt.Errorf("unsupported sql: %s", query)
}

func logWarns(warns []error) {
	for _, warn := range warns {
		log.Info(warn.Error())
	}
}

func newHandler(s types.ProxyComponent) driver.Handler {
	h := &handler{s: s}
	return h
}
