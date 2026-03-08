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

package milvusclient

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type LoadTask struct {
	client         *Client
	collectionName string
	partitionNames []string
	interval       time.Duration
	refresh        bool
}

func (t *LoadTask) Await(ctx context.Context) error {
	timer := time.NewTimer(t.interval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			loaded := false
			refreshed := false
			err := t.client.callService(func(milvusService milvuspb.MilvusServiceClient) error {
				resp, err := milvusService.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
					CollectionName: t.collectionName,
					PartitionNames: t.partitionNames,
				})
				if err = merr.CheckRPCCall(resp, err); err != nil {
					return err
				}
				loaded = resp.GetProgress() == 100
				refreshed = resp.GetRefreshProgress() == 100
				return nil
			})
			if err != nil {
				return err
			}
			if (loaded && !t.refresh) || (refreshed && t.refresh) {
				return nil
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(t.interval)
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
			refresh:        option.IsRefresh(),
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
			refresh:        option.IsRefresh(),
		}

		return nil
	})
	return task, err
}

func (c *Client) GetLoadState(ctx context.Context, option GetLoadStateOption, callOptions ...grpc.CallOption) (entity.LoadState, error) {
	req := option.Request()

	var state entity.LoadState
	var err error

	if err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.GetLoadState(ctx, req, callOptions...)
		state.State = entity.LoadStateCode(resp.GetState())
		return merr.CheckRPCCall(resp, err)
	}); err != nil {
		return state, err
	}

	// get progress if state is loading
	if state.State == entity.LoadStateLoading {
		err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
			resp, err := milvusService.GetLoadingProgress(ctx, option.ProgressRequest(), callOptions...)
			if err := merr.CheckRPCCall(resp, err); err != nil {
				return err
			}

			state.Progress = resp.GetProgress()
			return nil
		})
	}
	return state, err
}

func (c *Client) ReleaseCollection(ctx context.Context, option ReleaseCollectionOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ReleaseCollection(ctx, req, callOptions...)

		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) ReleasePartitions(ctx context.Context, option ReleasePartitionsOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ReleasePartitions(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) RefreshLoad(ctx context.Context, option RefreshLoadOption, callOptions ...grpc.CallOption) (LoadTask, error) {
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
			refresh:        true,
		}
		return nil
	})
	return task, err
}

type FlushTask struct {
	client             *Client
	collectionName     string
	segmentIDs         []int64
	flusheSegIDs       []int64
	flushTs            uint64
	channelCheckpoints map[string]*msgpb.MsgPosition
	interval           time.Duration
}

func (t *FlushTask) Await(ctx context.Context) error {
	timer := time.NewTimer(t.interval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			flushed := false
			err := t.client.callService(func(milvusService milvuspb.MilvusServiceClient) error {
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
			if err != nil {
				return err
			}
			if flushed {
				return nil
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(t.interval)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (t *FlushTask) GetFlushStats() (segIDs []int64, flushSegIDs []int64, flushTs uint64, channelCheckpoints map[string]*msgpb.MsgPosition) {
	return t.segmentIDs, t.flusheSegIDs, t.flushTs, t.channelCheckpoints
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
			client:             c,
			collectionName:     collectionName,
			segmentIDs:         resp.GetCollSegIDs()[collectionName].GetData(),
			flusheSegIDs:       resp.GetFlushCollSegIDs()[collectionName].GetData(),
			flushTs:            resp.GetCollFlushTs()[collectionName],
			channelCheckpoints: resp.GetChannelCps(),
			interval:           option.CheckInterval(),
		}

		return nil
	})
	return task, err
}

func (c *Client) Compact(ctx context.Context, option CompactOption, callOptions ...grpc.CallOption) (int64, error) {
	req := option.Request()

	var jobID int64

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ManualCompaction(ctx, req, callOptions...)
		jobID = resp.GetCompactionID()
		return merr.CheckRPCCall(resp, err)
	})
	return jobID, err
}

func (c *Client) GetCompactionState(ctx context.Context, option GetCompactionStateOption, callOptions ...grpc.CallOption) (entity.CompactionState, error) {
	req := option.Request()

	var status entity.CompactionState

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.GetCompactionState(ctx, req, callOptions...)
		status = entity.CompactionState(resp.GetState())
		return merr.CheckRPCCall(resp, err)
	})
	return status, err
}
