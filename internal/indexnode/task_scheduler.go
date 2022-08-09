package indexnode

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"go.uber.org/zap"
)

type taskScheduler struct {
	taskchan chan task
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewTaskScheduler(ctx context.Context, cap int) *taskScheduler {
	newctx, cancel := context.WithCancel(ctx)
	return &taskScheduler{
		taskchan: make(chan task, cap),
		ctx:      newctx,
		cancel:   cancel,
	}
}

func (s *taskScheduler) Enqueue(t task) error {
	ctx := t.Ctx()
	if err := t.OnEnqueue(ctx); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return cancelErr
	case s.taskchan <- t:
		return nil
	}
}

func (s *taskScheduler) GetPendingJob() int {
	return len(s.taskchan)
}

func (s *taskScheduler) indexBuildLoop() {
	log.Debug("IndexNode TaskScheduler start build loop ...")
	defer log.Warn("index build loop stopped")
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case t, ok := <-s.taskchan:
			if !ok {
				log.Error("task chan closed unexpectedly")
				return
			}
			s.doBuild(t)
		}
	}
}

func (s *taskScheduler) doBuild(t task) {
	wrap := func(fn func(ctx context.Context) error) error {
		select {
		case <-t.Ctx().Done():
			return cancelErr
		default:
			return fn(t.Ctx())
		}
	}
	defer func() {
		t.Reset()
		debug.FreeOSMemory()
	}()
	piplines := []func(context.Context) error{t.Prepare, t.LoadData, t.BuildIndex, t.SaveIndexFiles}
	for _, fn := range piplines {
		if err := wrap(fn); err != nil {
			if err == cancelErr {
				logutil.Logger(t.Ctx()).Warn("index build task cancelled", zap.String("task", t.Name()))
				t.SetState(commonpb.IndexState_Abandoned)
			} else if errors.Is(err, ErrNoSuchKey) {
				t.SetState(commonpb.IndexState_Failed)
			} else {
				t.SetState(commonpb.IndexState_Unissued)
			}
			return
		}
	}
	t.SetState(commonpb.IndexState_Finished)
}

func (s *taskScheduler) Start() {
	s.wg.Add(1)
	go s.indexBuildLoop()
}

func (s *taskScheduler) Close() {
	s.cancel()
	s.wg.Wait()
}
