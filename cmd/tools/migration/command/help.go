package command

import "fmt"

var (
	usageLineV2 = fmt.Sprintf("Usage:\n"+
		"%s\n", runLineV2)

	runLineV2 = `
migration -config=config.yaml
`
)
