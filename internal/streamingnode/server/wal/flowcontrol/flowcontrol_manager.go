package flowcontrol

import (
	"context"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type flowcontrolState int

const (
	stateNormal flowcontrolState = iota
	stateThrottling
	stateDeny

	defaultBurstSize = 100 * 1024 * 1024 // 100MB, it's enough for all message to pass.
)

var (
	once                       sync.Once
	flowcontrolManagerInstance *flowcontrolManager
)

func (s flowcontrolState) String() string {
	switch s {
	case stateNormal:
		return "normal"
	case stateThrottling:
		return "throttling"
	case stateDeny:
		return "deny"
	default:
		return "unknown"
	}
}

// RequestBytes blocks the caller until the specified number of bytes can be sent.
func RequestBytes(ctx context.Context, msg message.MutableMessage) error {
	once.Do(func() {
		flowcontrolManagerInstance = newFlowcontrolManager()
		listener := &hardware.SystemMetricsListener{
			Cooldown: 0,
			Condition: func(sm hardware.SystemMetrics) bool {
				return true
			},
			Callback: flowcontrolManagerInstance.stateChange,
		}
		hardware.RegisterSystemMetricsListener(listener)
	})
	return flowcontrolManagerInstance.RequestBytes(ctx, msg)
}

// newFlowcontrolManager creates a new flow control manager.
func newFlowcontrolManager() *flowcontrolManager {
	cfg, err := newConfig()
	if err != nil {
		panic(err)
	}
	fm := &flowcontrolManager{
		cfg:           cfg,
		limiter:       rate.NewLimiter(rate.Inf, 100*1024*1024),
		state:         stateNormal,
		metricsHelper: newMetricsHelper(),
	}
	fm.SetLogger(resource.Resource().Logger().With(zap.String(log.FieldNameComponent, "flowcontrol")))
	fm.metricsHelper.ObserveStateChange(fm.state)
	return fm
}

// flowcontrolManager is a manager to control the wal right flow.
type flowcontrolManager struct {
	log.Binder
	cfg           config
	limiter       *rate.Limiter
	state         flowcontrolState
	metricsHelper *metricsHelper
}

// RequestBytes blocks the caller until the specified number of bytes can be sent.
func (fm *flowcontrolManager) RequestBytes(ctx context.Context, msg message.MutableMessage) (err error) {
	// TODO:
	// - fairness between different source and pchannel for flow control.
	// - the transaction expiration may be affected by the flow control.

	// only data message will be flow controlled.
	if !msg.MessageType().IsData() {
		return nil
	}
	g := fm.metricsHelper.StartFlowControl()
	defer func() {
		g.Done(err)
	}()

	err = fm.limiter.WaitN(ctx, msg.EstimateSize())
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	if err != nil {
		if strings.Contains(err.Error(), "exceed context deadline") {
			// The rater doesn't export the error, so we can only make a string check here.
			return context.DeadlineExceeded
		}
		// if the flow control is denied, we should return a flow control error.
		return status.NewFlowControlDenied("")
	}
	return nil
}

// stateChange is a callback function that is called when the system metrics change.
func (fm *flowcontrolManager) stateChange(sm hardware.SystemMetrics) {
	fm.updateConfig()
	usage := sm.UsedRatio()
	pervState := fm.state
	switch fm.state {
	case stateNormal:
		if usage > fm.cfg.DenyThreshold {
			fm.intoDeny()
		} else if usage > fm.cfg.HwmThreshold {
			fm.intoThrottling()
		}
	case stateThrottling:
		if usage > fm.cfg.DenyThreshold {
			fm.intoDeny()
		} else if usage < fm.cfg.LwmThreshold {
			fm.intoNormal()
		}
	case stateDeny:
		if usage < fm.cfg.LwmThreshold {
			fm.intoNormal()
		}
	}
	if fm.state != pervState {
		fm.Logger().Info("flow control state changed", zap.Stringer("prev", pervState), zap.Stringer("current", fm.state))
		fm.metricsHelper.ObserveStateChange(fm.state)
	}
}

// intoDeny sets the flow control manager to deny state.
func (fm *flowcontrolManager) intoDeny() {
	fm.state = stateDeny
	fm.limiter.SetLimit(rate.Limit(0))
	fm.limiter.SetBurst(0)
}

// intoThrottling sets the flow control manager to throttling state.
func (fm *flowcontrolManager) intoThrottling() {
	fm.state = stateThrottling
	fm.limiter.SetLimit(fm.cfg.ThrottlingRate)
	fm.limiter.SetBurst(defaultBurstSize)
}

// intoNormal sets the flow control manager to normal state.
func (fm *flowcontrolManager) intoNormal() {
	fm.state = stateNormal
	fm.limiter.SetLimit(fm.cfg.DefaultRate)
	fm.limiter.SetBurst(defaultBurstSize)
}

// updateConfig updates the configuration of the flow control manager.
func (fm *flowcontrolManager) updateConfig() {
	newCfg, err := newConfig()
	if err != nil {
		fm.Logger().Warn("failed to update flow control manager config", zap.Any("new", newCfg), zap.Error(err))
		return
	}
	if fm.cfg == newCfg {
		return
	}
	fm.Logger().Info("flow control manager config updated", zap.Any("old", fm.cfg), zap.Any("new", newCfg))
	fm.cfg = newCfg
	if fm.state == stateThrottling {
		fm.limiter.SetLimit(newCfg.ThrottlingRate)
	} else if fm.state == stateNormal {
		fm.limiter.SetLimit(newCfg.DefaultRate)
	}
}
