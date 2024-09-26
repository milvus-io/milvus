package planparserv2

import (
	"fmt"
	"strconv"

	"github.com/antlr/antlr4/runtime/Go/antlr"
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
	l.err = fmt.Errorf("line " + strconv.Itoa(line) + ":" + strconv.Itoa(column) + " " + msg)
}

func (l *errorListenerImpl) Error() error {
	return l.err
}
