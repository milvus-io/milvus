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

package client

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type LoadTask struct {
	client         *Client
	collectionName string
	partitionNames []string
	interval       time.Duration
}

func (t *LoadTask) Await(ctx context.Context) error {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			loaded := false
			t.client.callService(func(milvusService milvuspb.MilvusServiceClient) error {
				resp, err := milvusService.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
					CollectionName: t.collectionName,
					PartitionNames: t.partitionNames,
				})
				if err = merr.CheckRPCCall(resp, err); err != nil {
					return err
				}
				loaded = resp.GetProgress() == 100
				return nil
			})
			if loaded {
				return nil
			}
			ticker.Reset(t.interval)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Client) LoadCollection(ctx context.Context, option LoadCollectionOption, callOptions ...grpc.CallOption) (LoadTask, error) {
	req := option.Request()

	var task LoadTask

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.LoadCollection(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		task = LoadTask{
			client:         c,
			collectionName: req.GetCollectionName(),
			interval:       option.CheckInterval(),
		}

		return nil
	})
	return task, err
}

func (c *Client) LoadPartitions(ctx context.Context, option LoadPartitionsOption, callOptions ...grpc.CallOption) (LoadTask, error) {
	req := option.Request()

	var task LoadTask

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.LoadPartitions(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		task = LoadTask{
			client:         c,
			collectionName: req.GetCollectionName(),
			partitionNames: req.GetPartitionNames(),
			interval:       option.CheckInterval(),
		}

		return nil
	})
	return task, err
}

type FlushTask struct {
	client         *Client
	collectionName string
	segmentIDs     []int64
	flushTs        uint64
	interval       time.Duration
}

func (t *FlushTask) Await(ctx context.Context) error {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			flushed := false
			t.client.callService(func(milvusService milvuspb.MilvusServiceClient) error {
				resp, err := milvusService.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
					CollectionName: t.collectionName,
					SegmentIDs:     t.segmentIDs,
					FlushTs:        t.flushTs,
				})
				err = merr.CheckRPCCall(resp, err)
				if err != nil {
					return err
				}
				flushed = resp.GetFlushed()

				return nil
			})
			if flushed {
				return nil
			}
			ticker.Reset(t.interval)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Client) Flush(ctx context.Context, option FlushOption, callOptions ...grpc.CallOption) (*FlushTask, error) {
	req := option.Request()
	collectionName := option.CollectionName()
	var task *FlushTask

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Flush(ctx, req, callOptions...)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}

		task = &FlushTask{
			client:         c,
			collectionName: collectionName,
			segmentIDs:     resp.GetCollSegIDs()[collectionName].GetData(),
			flushTs:        resp.GetCollFlushTs()[collectionName],
			interval:       option.CheckInterval(),
		}

		return nil
	})
	return task, err
}
