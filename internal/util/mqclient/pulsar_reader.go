// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqclient

import (
	"context"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

// PulsarReader wraps pulsar reader into a mqclient.Consumer
type PulsarReader struct {
	sync.Mutex
	client       pulsar.Client
	topicName    string
	readerName   string
	initPosition SubscriptionInitialPosition
	worker       *pulsarReaderWorker
	msgChannel   chan Message
}

// pulsarReaderWorker handles single seek/start work flow
// its lifetime is controlled by
type pulsarReaderWorker struct {
	r         pulsar.Reader
	closeCh   chan struct{}
	wg        sync.WaitGroup
	startOnce sync.Once
	closeOnce sync.Once
}

// work consumes msg from reader if not nil and put it into channel
func (pr *pulsarReaderWorker) work(result chan<- Message) {
	defer pr.wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			msg, err := pr.r.Next(ctx)
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err != nil {
				log.Warn("pulsar reader next error", zap.Error(err))
			}
			if msg != nil {
				result <- &pulsarReaderMessage{msg: msg}
			}
		}
	}()
	<-pr.closeCh
}

func (pr *pulsarReaderWorker) start(result chan<- Message) {
	pr.startOnce.Do(func() {
		pr.wg.Add(1)
		go pr.work(result)
	})
}

func (pr *pulsarReaderWorker) stop() {
	pr.closeOnce.Do(func() {
		close(pr.closeCh)
	})
	pr.wg.Wait()
}

// Sbuscription returns the subscription for the consumer.
func (pr *PulsarReader) Subscription() string {
	return pr.topicName
}

// Chan returns message channel.
func (pr *PulsarReader) Chan() <-chan Message {
	pr.Lock()
	defer pr.Unlock()
	// lasy start
	if pr.worker == nil {

		var messageID pulsar.MessageID
		switch pr.initPosition {
		case SubscriptionPositionEarliest:
			messageID = pulsar.EarliestMessageID()
			//patchEarliestMessageID(&messageID)
		case SubscriptionPositionLatest:
			messageID = pulsar.LatestMessageID()
		}
		reader, err := pr.client.CreateReader(pulsar.ReaderOptions{
			Topic:          pr.topicName,
			Name:           pr.readerName,
			StartMessageID: messageID,
		})
		if err != nil {
			// not handling error due to function signature
			// the following work will quit since pr.r is nil
			log.Warn("PulsarReader fail to create", zap.Error(err))
			// close msgChannel to notify caller
			// this will impact the whole PulsarReader, `Seek` after this failure will not work
			close(pr.msgChannel)
			return pr.msgChannel
		}

		pr.worker = &pulsarReaderWorker{
			r:       reader,
			closeCh: make(chan struct{}),
		}
		pr.worker.start(pr.msgChannel)
	}
	return pr.msgChannel
}

// Seek to the MessageID position.
func (pr *PulsarReader) Seek(id MessageID) (err error) {
	messageID := id.(*pulsarID).messageID

	// create reader first, if err occurs, skip stop current worker
	reader, err := pr.client.CreateReader(pulsar.ReaderOptions{
		Topic:                   pr.topicName,
		Name:                    pr.readerName,
		StartMessageID:          messageID,
		StartMessageIDInclusive: false, // skip the message of the EXACT msg id
	})

	if err != nil {
		return err
	}

	pr.Lock()
	defer pr.Unlock()
	// if there is a worker before, stop it
	if pr.worker != nil {
		pr.worker.stop()
		pr.worker = nil
	}

	// drain message buffer, to support seek after starting consume
	drained := false
	for !drained {
		select {
		case <-pr.msgChannel:
		default:
			drained = true
		}
	}

	pr.worker = &pulsarReaderWorker{
		r:       reader,
		closeCh: make(chan struct{}),
	}
	pr.worker.start(pr.msgChannel)

	return err
}

// Ack makes sure that msg is received.
func (pr *PulsarReader) Ack(_ Message) {
	// Reader does not perform ack here
	// Reader ack msg automatically
}

// ConsumeAfterSeek defines the behavior whether to consume after seeking is done
func (pr *PulsarReader) ConsumeAfterSeek() bool {
	return false
}

// Close consumer
func (pr *PulsarReader) Close() {
	pr.Lock()
	defer pr.Unlock()
	if pr.worker != nil {
		pr.worker.stop()
		pr.worker = nil
	}
}
