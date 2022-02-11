package errorutil

import (
	"fmt"
	"strings"
)

// ErrorList for print error log
type ErrorList []error

// Error method return an string representation of retry error list.
func (el ErrorList) Error() string {
	limit := 10
	var builder strings.Builder
	builder.WriteString("All attempts results:\n")
	for index, err := range el {
		// if early termination happens
		if err == nil {
			break
		}
		if index > limit {
			break
		}
		builder.WriteString(fmt.Sprintf("attempt #%d:%s\n", index+1, err.Error()))
	}
	return builder.String()
}
