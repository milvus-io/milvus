package planparserv2

import (
	"github.com/antlr4-go/antlr/v4"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type errorListener interface {
	antlr.ErrorListener
	Error() error
}

type errorListenerImpl struct {
	*antlr.DefaultErrorListener
	err error
}

func (l *errorListenerImpl) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	l.err = merr.WrapErrQueryPlanMsg("line %d:%d %s", line, column, msg)
}

func (l *errorListenerImpl) Error() error {
	return l.err
}
