package utility

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// LoggableAlterWALState renders an AlterWALState without its Configs values.
//
// AlterWALState.Configs is the target broker's own configuration, taken
// verbatim from the /management/wal/alter request body, so it is where
// sasl.password and ssl.key.pem style material arrives. Unlike a config value
// read through config.Manager it carries no declared sensitivity metadata, and
// unlike the request itself this copy is persisted into the WAL checkpoint —
// so logging it prints the credential again on every restart, forever. The
// option names are what makes a broker misconfiguration diagnosable and they
// are not secret, so they stay.
func LoggableAlterWALState(state *streamingpb.AlterWALState) any {
	if state == nil {
		return nil
	}
	return struct {
		Stage         string   `json:"stage"`
		TargetWalName string   `json:"targetWalName"`
		ConfigKeys    []string `json:"configKeys"`
	}{
		Stage:         state.GetStage().String(),
		TargetWalName: state.GetTargetWalName().String(),
		ConfigKeys:    lo.Keys(state.GetConfigs()),
	}
}
