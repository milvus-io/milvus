//go:build test
// +build test

package walimplstest

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

var _ walimpls.WALImpls = &walImpls{}

type walImpls struct {
	helper.WALHelper
	datas *messageLog
}

func (w *walImpls) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	return w.datas.Append(ctx, msg)
}

func (w *walImpls) Read(ctx context.Context, opts walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	offset := int64(0)
	switch policy := opts.DeliverPolicy.Policy.(type) {
	case *logpb.DeliverPolicy_All:
		offset = 0
	case *logpb.DeliverPolicy_Latest:
		offset = w.datas.Len()
	case *logpb.DeliverPolicy_StartFrom:
		id, err := unmarshalTestMessageID(policy.StartFrom.Id)
		if err != nil {
			return nil, err
		}
		offset = int64(id)
	case *logpb.DeliverPolicy_StartAfter:
		id, err := unmarshalTestMessageID(policy.StartAfter.Id)
		if err != nil {
			return nil, err
		}
		offset = int64(id + 1)
	}
	return newScannerImpls(
		opts, w.datas, int(offset),
	), nil
}

func (w *walImpls) Close() {
}
