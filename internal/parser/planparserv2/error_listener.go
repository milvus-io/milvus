package planparserv2

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
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
	l.err = fmt.Errorf("line %d:%d %s", line, column, msg)
}

func (l *errorListenerImpl) Error() error {
	return l.err
}
