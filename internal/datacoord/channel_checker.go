package datacoord

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

const (
	checkChannelOpProgressInterval = 200 * time.Millisecond
)

type ChannelChecker struct {
	session       *SessionManager
	checkers      typeutil.ConcurrentMap[string, *checkCache] // channel -> checkerCache
	checkerWaiter sync.WaitGroup

	resultWatcher chan *checkResult
}

type checkCache struct {
	closeCh chan struct{}
}

type checkResult struct {
	channel  string
	state    datapb.ChannelWatchState
	singleOp *SingleChannelOp
}

func NewChannelChecker(session *SessionManager) *ChannelChecker {
	return &ChannelChecker{
		session:       session,
		resultWatcher: make(chan *checkResult, 100),
	}
}

func (c *ChannelChecker) Close() {
	c.checkers.Range(func(key string, value *checkCache) bool {
		c.Remove(key)
		return true
	})

	c.checkerWaiter.Wait()
}

func (c *ChannelChecker) AddChecker(channel string, ch chan struct{}) {
	c.checkers.Insert(channel, &checkCache{ch})
}

func (c *ChannelChecker) Submit(ctx context.Context, singleOp *SingleChannelOp, skipCheck bool) {
	if skipCheck {
		c.FinishWithResult(&checkResult{
			channel:  singleOp.Channel.Name,
			state:    getFailState(singleOp.ChannelWatchInfo.GetState()),
			singleOp: singleOp,
		})
		return
	}

	done := make(chan struct{})
	c.AddChecker(singleOp.Channel.Name, done)

	c.checkerWaiter.Add(1)
	go func() {
		defer c.checkerWaiter.Done()
		log := log.With(
			zap.String("channel", singleOp.Channel.Name),
			zap.Int64("nodeID", singleOp.NodeID),
			zap.String("state", singleOp.ChannelWatchInfo.GetState().String()),
		)
		ticker := time.NewTicker(checkChannelOpProgressInterval)
		timeoutTicker := time.NewTicker(Params.DataCoordCfg.WatchTimeoutInterval.GetAsDuration(time.Second))
		defer ticker.Stop()
		defer timeoutTicker.Stop()
		for {
			select {
			case <-timeoutTicker.C:
				// timeout and no progress
				log.Warn("channel operation timeout")
				c.FinishWithResult(&checkResult{
					channel:  singleOp.Channel.Name,
					state:    getFailState(singleOp.ChannelWatchInfo.GetState()),
					singleOp: singleOp,
				})
				return
			case <-ticker.C:
				progress, err := c.session.CheckChannelOperationProgress(ctx, singleOp.NodeID, singleOp.ChannelWatchInfo)
				if err != nil {
					log.Warn("fail to check channel operation progress, mark this operation fail")
					// finish with fail state
					c.FinishWithResult(&checkResult{
						channel:  singleOp.Channel.Name,
						state:    getFailState(singleOp.ChannelWatchInfo.GetState()),
						singleOp: singleOp,
					})
					return
				}

				// finish operation and return
				if progress == 100 {
					log.Info("channel operation succeeded with progress 100")
					successState := getSuccessState(singleOp.ChannelWatchInfo.State)
					singleOp.Update(successState)
					c.FinishWithResult(&checkResult{
						channel:  singleOp.Channel.Name,
						state:    successState,
						singleOp: singleOp,
					})
					return
				}

				// reset ticker with updated progress
				if progress > 0 && singleOp.ChannelWatchInfo.GetProgress() < progress {
					log.Info("channel operation got new progress and reset the timer",
						zap.Int32("got progress", progress),
						zap.Int32("provious progress", singleOp.ChannelWatchInfo.Progress),
					)
					singleOp.ChannelWatchInfo.Progress = progress
					timeoutTicker.Reset(Params.DataCoordCfg.WatchTimeoutInterval.GetAsDuration(time.Second))
				}

			case <-done:
				c.Remove(singleOp.Channel.Name)
				log.Info("channel operation canceled")
				return
			}
		}
	}()
}

func (c *ChannelChecker) Remove(channel string) {
	c.checkers.GetAndRemove(channel)
}

func (c *ChannelChecker) Exists(channel string) bool {
	_, ok := c.checkers.Get(channel)
	return ok
}

func (c *ChannelChecker) Empty() bool {
	return c.checkers.Len() == 0 && len(c.resultWatcher) == 0
}

func (c *ChannelChecker) Watcher() chan *checkResult {
	return c.resultWatcher
}

func (c *ChannelChecker) FinishWithResult(result *checkResult) {
	loaded, ok := c.checkers.GetAndRemove(result.channel)
	if ok {
		close(loaded.closeCh)
	}
	c.resultWatcher <- result
}

func getSuccessState(state datapb.ChannelWatchState) datapb.ChannelWatchState {
	switch state {
	case datapb.ChannelWatchState_ToWatch:
		return datapb.ChannelWatchState_WatchSuccess
	case datapb.ChannelWatchState_ToRelease:
		return datapb.ChannelWatchState_ReleaseSuccess
	default:
		return datapb.ChannelWatchState_Complete
	}
}

func getFailState(state datapb.ChannelWatchState) datapb.ChannelWatchState {
	switch state {
	case datapb.ChannelWatchState_ToWatch:
		return datapb.ChannelWatchState_WatchFailure
	case datapb.ChannelWatchState_ToRelease:
		return datapb.ChannelWatchState_ReleaseFailure
	default:
		return datapb.ChannelWatchState_ReleaseFailure
	}
}
