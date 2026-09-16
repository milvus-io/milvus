package scheduler

import (
	"context"
	"math/rand"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

var (
	_ Task      = &MockTask{}
	_ MergeTask = &MockTask{}
)

type mockTaskConfig struct {
	ctx         context.Context
	mergeAble   bool
	nq          int64
	username    string
	executeCost time.Duration
	execution   func(ctx context.Context) error
	// prune, when set, replaces the default PruneCancelled behaviour.
	prune func() Task
}

func newMockTask(c mockTaskConfig) Task {
	if c.ctx == nil {
		c.ctx = context.Background()
	}
	if c.nq == 0 {
		c.nq = 1
	}
	if c.executeCost == 0 {
		c.executeCost = time.Duration((rand.Int31n(4) + 1) * int32(time.Second))
	}
	return &MockTask{
		ctx:         c.ctx,
		executeCost: c.executeCost,
		notifier:    make(chan error, 1),
		mergeAble:   c.mergeAble,
		nq:          c.nq,
		minNQ:       c.nq,
		username:    c.username,
		execution:   c.execution,
		prune:       c.prune,
		tr:          timerecord.NewTimeRecorderWithTrace(c.ctx, "searchTask"),
	}
}

type MockTask struct {
	ctx         context.Context
	executeCost time.Duration
	notifier    chan error
	mergeAble   bool
	nq          int64
	minNQ       int64
	username    string
	execution   func(ctx context.Context) error
	prune       func() Task
	tr          *timerecord.TimeRecorder
}

// PruneCancelled implements PrunableTask. Without a custom prune function it
// behaves like a standalone task: dropped with its own ctx error when
// cancelled, kept otherwise.
func (t *MockTask) PruneCancelled() Task {
	if t.prune != nil {
		return t.prune()
	}
	if err := t.ctx.Err(); err != nil {
		t.Done(err)
		return nil
	}
	return t
}

// QueryTypeMetricLabel Return Metric label for metric label.
func (t *MockTask) QueryTypeMetricLabel() string {
	return "mock"
}

func (t *MockTask) Username() string {
	return t.username
}

func (t *MockTask) IsGpuIndex() bool {
	return false
}

func (t *MockTask) TimeRecorder() *timerecord.TimeRecorder {
	return t.tr
}

func (t *MockTask) PreExecute() error {
	return nil
}

func (t *MockTask) Execute() error {
	var err error
	time.Sleep(t.executeCost)
	if t.execution != nil {
		err = t.execution(t.ctx)
	}
	return err
}

func (t *MockTask) Done(err error) {
	t.notifier <- err
}

func (t *MockTask) Wait() error {
	return <-t.notifier
}

// Return the context of task.
func (t *MockTask) Context() context.Context {
	return t.ctx
}

func (t *MockTask) MergeWith(t2 Task) bool {
	switch t2 := t2.(type) {
	case *MockTask:
		if t.mergeAble && t2.mergeAble {
			t.nq += t2.nq
			if t2.MinNQ() < t.minNQ {
				t.minNQ = t2.MinNQ()
			}
			t.executeCost += t2.executeCost
			return true
		}
	}
	return false
}

func (t *MockTask) SearchResult() *internalpb.SearchResults {
	return nil
}

func (t *MockTask) NQ() int64 {
	return t.nq
}

func (t *MockTask) MinNQ() int64 {
	return t.minNQ
}
