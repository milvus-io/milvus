package main

import "fmt"

var (
	usageLine = fmt.Sprintf("Usage:\n"+
		"%s\n%s\n", runLine, dryRunLine)

	runLine = `
migration run [flags]
	Run Milvus migration.
[flags]
	-sourceYaml ''
		Source yaml configurations.
	-sourceVersion ''
		Source version of Milvus, 2.1.0 for example.
	-targetYaml ''
		Target yaml configurations.
	-targetVersion ''
		Target version of Milvus, 2.2.0 for example.
`
	dryRunLine = `
migration dry-run [flags]
	Dry-run Milvus migration.
[flags]
	-sourceYaml ''
		Source yaml configurations.
	-sourceVersion ''
		Source version of Milvus, 2.1.0 for example.
	-targetYaml ''
		Target yaml configurations.
	-targetVersion ''
		Target version of Milvus, 2.2.0 for example.
`
)
