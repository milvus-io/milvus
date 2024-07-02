//go:build test
// +build test

package walimplstest

import (
	"context"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/helper"
)

var _ walimpls.WALImpls = &walImpls{}

type walImpls struct {
	helper.WALHelper
	datas *messageLog
}

func (w *walImpls) WALName() string {
	return WALName
}

func (w *walImpls) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	return w.datas.Append(ctx, msg)
}

func (w *walImpls) Read(ctx context.Context, opts walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	offset := int64(0)
	switch opts.DeliverPolicy.Policy() {
	case options.DeliverPolicyTypeAll:
		offset = 0
	case options.DeliverPolicyTypeLatest:
		offset = w.datas.Len()
	case options.DeliverPolicyTypeStartFrom:
		offset = int64(opts.DeliverPolicy.MessageID().(testMessageID))
	case options.DeliverPolicyTypeStartAfter:
		offset = int64(opts.DeliverPolicy.MessageID().(testMessageID)) + 1
	}
	return newScannerImpls(
		opts, w.datas, int(offset),
	), nil
}

func (w *walImpls) Close() {
}
