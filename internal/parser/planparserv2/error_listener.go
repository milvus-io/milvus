package planparserv2

import (
	"fmt"
	"strconv"

	"github.com/antlr4-go/antlr/v4"
)

type errorListener struct {
	*antlr.DefaultErrorListener
	err error
}

func (l *errorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	l.err = fmt.Errorf("line " + strconv.Itoa(line) + ":" + strconv.Itoa(column) + " " + msg)
}
