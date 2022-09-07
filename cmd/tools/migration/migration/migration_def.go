package migration

import (
	"fmt"

	"github.com/blang/semver/v4"
)

type migrationDef struct {
	source *backendOpt
	target *backendOpt
}

func (d migrationDef) show() {
	fmt.Printf("source version: %v, target version: %v\n", d.source.version, d.target.version)
}

func ParseToMigrationDef(sourceYaml, targetYaml string, sourceVersion, targetVersion string) (*migrationDef, error) {
	sourceV, err := semver.Parse(sourceVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source version, version: %s, error : %s", sourceVersion, err.Error())
	}
	targetV, err := semver.Parse(targetVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to parse target version, version: %s, error : %s", targetVersion, err.Error())
	}
	source := ReadYamlAsBackendOpt(sourceYaml)
	target := ReadYamlAsBackendOpt(targetYaml)
	source.SetVersion(sourceV)
	target.SetVersion(targetV)
	def := &migrationDef{
		source: source,
		target: target,
	}
	return def, nil
}
