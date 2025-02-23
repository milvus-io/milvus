//go:build test
// +build test

package walimplstest

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
		logs: make([][]byte, 0),
	}
}

type messageLog struct {
	cond *syncutil.ContextCond
	id   int64
	logs [][]byte
}

type entry struct {
	ID         int64
	Payload    []byte
	Properties map[string]string
}

func (l *messageLog) Append(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
	l.cond.LockAndBroadcast()
	defer l.cond.L.Unlock()
	id := l.id
	newEntry := entry{
		ID:         id,
		Payload:    msg.Payload(),
		Properties: msg.Properties().ToRawMap(),
	}
	data, err := json.Marshal(newEntry)
	if err != nil {
		return nil, err
	}

	l.id++
	l.logs = append(l.logs, data)
	return NewTestMessageID(id), nil
}

func (l *messageLog) ReadAt(ctx context.Context, idx int) (message.ImmutableMessage, error) {
	l.cond.L.Lock()

	for idx >= len(l.logs) {
		if err := l.cond.Wait(ctx); err != nil {
			return nil, err
		}
	}
	defer l.cond.L.Unlock()

	data := l.logs[idx]
	var newEntry entry
	if err := json.Unmarshal(data, &newEntry); err != nil {
		return nil, err
	}
	return message.NewImmutableMesasge(
		NewTestMessageID(newEntry.ID),
		newEntry.Payload,
		newEntry.Properties,
	), nil
}

func (l *messageLog) Len() int64 {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()
	return int64(len(l.logs))
}
