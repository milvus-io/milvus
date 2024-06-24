//go:build test
// +build test

package walimplstest

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
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
	case *streamingpb.DeliverPolicy_All:
		offset = 0
	case *streamingpb.DeliverPolicy_Latest:
		offset = w.datas.Len()
	case *streamingpb.DeliverPolicy_StartFrom:
		id, err := unmarshalTestMessageID(policy.StartFrom.Id)
		if err != nil {
			return nil, err
		}
		offset = int64(id)
	case *streamingpb.DeliverPolicy_StartAfter:
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
