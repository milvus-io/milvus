// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"go.uber.org/zap"
)

type inputStage struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID

	queryStream msgstream.MsgStream
	lbOutput    chan *msgstream.LoadBalanceSegmentsMsg
	queryOutput chan queryMsg
}

func newInputStage(ctx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	queryStream msgstream.MsgStream,
	lbOutput chan *msgstream.LoadBalanceSegmentsMsg,
	queryOutput chan queryMsg) *inputStage {

	return &inputStage{
		ctx:          ctx,
		cancel:       cancel,
		collectionID: collectionID,
		queryStream:  queryStream,
		lbOutput:     lbOutput,
		queryOutput:  queryOutput,
	}
}

func (q *inputStage) start() {
	for {
		select {
		case <-q.ctx.Done():
			log.Debug("stop inputStage", zap.Int64("collectionID", q.collectionID))
			return
		default:
			msgPack := q.queryStream.Consume()
			if msgPack == nil || len(msgPack.Msgs) <= 0 {
				msgPackNil := msgPack == nil
				msgPackEmpty := true
				if msgPack != nil {
					msgPackEmpty = len(msgPack.Msgs) <= 0
				}
				log.Debug("consume query message failed", zap.Any("msgPack is Nil", msgPackNil),
					zap.Any("msgPackEmpty", msgPackEmpty))
				continue
			}
			for _, msg := range msgPack.Msgs {
				switch sm := msg.(type) {
				case *msgstream.SearchMsg:
				case *msgstream.RetrieveMsg:
					q.queryOutput <- sm
				case *msgstream.LoadBalanceSegmentsMsg:
					q.lbOutput <- sm
				default:
					log.Warn("unsupported msg type in search channel", zap.Any("msg", sm))
				}
			}
		}
	}
}
