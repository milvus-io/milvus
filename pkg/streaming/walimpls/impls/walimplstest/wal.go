//go:build test
// +build test

package walimplstest

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	_                walimpls.WALImpls = &walImpls{}
	fenced                             = typeutil.NewConcurrentSet[string]()
	enableFenceError                   = atomic.NewBool(true)
)

// EnableFenced enables fenced mode for the given channel.
func EnableFenced(channel string) {
	fenced.Insert(channel)
}

// DisableFenced disables fenced mode for the given channel.
func DisableFenced(channel string) {
	fenced.Remove(channel)
}

type walImpls struct {
	helper.WALHelper
	datas *messageLog
}

func (w *walImpls) WALName() string {
	return WALName
}

func (w *walImpls) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("write on a wal that is not in read-write mode")
	}
	if fenced.Contain(w.Channel().Name) {
		return nil, errors.Mark(errors.New("err"), walimpls.ErrFenced)
	}
	if enableFenceError.Load() && rand.Int31n(30) == 0 {
		return nil, errors.New("random error")
	}
	return w.datas.Append(ctx, msg)
}

func (w *walImpls) Read(ctx context.Context, opts walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	offset := int64(0)
	switch t := opts.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_All:
		offset = 0
	case *streamingpb.DeliverPolicy_Latest:
		offset = w.datas.Len()
	case *streamingpb.DeliverPolicy_StartFrom:
		id, err := unmarshalTestMessageID(t.StartFrom.GetId())
		if err != nil {
			return nil, err
		}
		offset = int64(id)
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err := unmarshalTestMessageID(t.StartAfter.GetId())
		if err != nil {
			return nil, err
		}
		offset = int64(id) + 1
	}
	return newScannerImpls(
		opts, w.datas, int(offset),
	), nil
}

func (w *walImpls) Truncate(ctx context.Context, id message.MessageID) error {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("truncate on a wal that is not in read-write mode")
	}
	return nil
}

func (w *walImpls) Close() {
}
