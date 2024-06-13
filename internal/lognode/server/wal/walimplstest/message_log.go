//go:build test
// +build test

package walimplstest

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var logs = typeutil.NewConcurrentMap[string, *messageLog]()

func getOrCreateLogs(name string) *messageLog {
	l := newMessageLog()
	l, _ = logs.GetOrInsert(name, l)
	return l
}

func newMessageLog() *messageLog {
	return &messageLog{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
		id:   0,
		logs: make([]message.ImmutableMessage, 0),
	}
}

type messageLog struct {
	cond *syncutil.ContextCond
	id   int64
	logs []message.ImmutableMessage
}

func (l *messageLog) Append(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
	l.cond.LockAndBroadcast()
	defer l.cond.L.Unlock()
	newMessageID := NewTestMessageID(l.id)
	l.id++
	l.logs = append(l.logs, msg.IntoImmutableMessage(newMessageID))
	return newMessageID, nil
}

func (l *messageLog) ReadAt(ctx context.Context, idx int) (message.ImmutableMessage, error) {
	var msg message.ImmutableMessage
	l.cond.L.Lock()
	for idx >= len(l.logs) {
		if err := l.cond.Wait(ctx); err != nil {
			return nil, err
		}
	}
	msg = l.logs[idx]
	l.cond.L.Unlock()

	return msg, nil
}

func (l *messageLog) Len() int64 {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()
	return int64(len(l.logs))
}
