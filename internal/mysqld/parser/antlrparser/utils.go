package antlrparser

import (
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
)

func CaseInsensitiveEqual(s1, s2 string) bool {
	return strings.ToLower(s1) == strings.ToLower(s2)
}

// GetOriginalText don't work.
func GetOriginalText(ctx antlr.ParserRuleContext) string {
	if true {
		return ctx.GetText()
	}
	interval := ctx.GetSourceInterval()
	return ctx.GetStart().GetInputStream().GetTextFromInterval(interval)
}
