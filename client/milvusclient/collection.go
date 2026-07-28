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

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

// CreateCollection is the API for create a collection in Milvus.
func (c *Client) CreateCollection(ctx context.Context, option CreateCollectionOption, callOptions ...grpc.CallOption) error {
	// Client-side schema validation: options that expose Validate() get a pre-flight sanity check.
	if v, ok := option.(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	req := option.Request()

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.CreateCollection(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
	if err != nil {
		return err
	}

	indexes := option.Indexes()
	for _, indexOption := range indexes {
		task, err := c.CreateIndex(ctx, indexOption, callOptions...)
		if err != nil {
			return err
		}
		err = task.Await(ctx)
		if err != nil {
			return nil
		}
	}

	if option.IsFast() {
		task, err := c.LoadCollection(ctx, NewLoadCollectionOption(req.GetCollectionName()))
		if err != nil {
			return err
		}
		return task.Await(ctx)
	}

	return nil
}

func (c *Client) ListCollections(ctx context.Context, option ListCollectionOption, callOptions ...grpc.CallOption) (collectionNames []string, err error) {
	req := option.Request()
	err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ShowCollections(ctx, req, callOptions...)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}

		collectionNames = resp.GetCollectionNames()
		return nil
	})

	return collectionNames, err
}

func (c *Client) DescribeCollection(ctx context.Context, option DescribeCollectionOption, callOptions ...grpc.CallOption) (collection *entity.Collection, err error) {
	req := option.Request()
	err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DescribeCollection(ctx, req, callOptions...)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			return err
		}

		collection = &entity.Collection{
			ID:               resp.GetCollectionID(),
			Schema:           entity.NewSchema().ReadProto(resp.GetSchema()),
			PhysicalChannels: resp.GetPhysicalChannelNames(),
			VirtualChannels:  resp.GetVirtualChannelNames(),
			ConsistencyLevel: entity.ConsistencyLevel(resp.ConsistencyLevel),
			ShardNum:         resp.GetShardsNum(),
			Properties:       entity.KvPairsMap(resp.GetProperties()),
			UpdateTimestamp:  resp.GetUpdateTimestamp(),
		}
		collection.Name = collection.Schema.CollectionName
		return nil
	})

	return collection, err
}

func (c *Client) HasCollection(ctx context.Context, option HasCollectionOption, callOptions ...grpc.CallOption) (has bool, err error) {
	req := option.Request()
	err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DescribeCollection(ctx, req, callOptions...)
		err = merr.CheckRPCCall(resp, err)
		if err != nil {
			// ErrCollectionNotFound for collection not exist
			if errors.Is(err, merr.ErrCollectionNotFound) {
				return nil
			}
			return err
		}
		has = true
		return nil
	})
	return has, err
}

func (c *Client) DropCollection(ctx context.Context, option DropCollectionOption, callOptions ...grpc.CallOption) error {
	req := option.Request()
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DropCollection(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
	return err
}

func (c *Client) TruncateCollection(ctx context.Context, option TruncateCollectionOption, callOptions ...grpc.CallOption) error {
	req := option.Request()
	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.TruncateCollection(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) RenameCollection(ctx context.Context, option RenameCollectionOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.RenameCollection(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) AlterCollectionProperties(ctx context.Context, option AlterCollectionPropertiesOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.AlterCollection(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) DropCollectionProperties(ctx context.Context, option DropCollectionPropertiesOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.AlterCollection(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) AlterCollectionFieldProperty(ctx context.Context, option AlterCollectionFieldPropertiesOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.AlterCollectionField(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) GetCollectionStats(ctx context.Context, opt GetCollectionOption) (map[string]string, error) {
	var stats map[string]string
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.GetCollectionStatistics(ctx, opt.Request())
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		stats = entity.KvPairsMap(resp.GetStats())
		return nil
	})
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (c *Client) alterCollectionSchemaRequest(ctx context.Context, req *milvuspb.AlterCollectionSchemaRequest, callOpts ...grpc.CallOption) error {
	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.AlterCollectionSchema(ctx, req, callOpts...)
		if resp == nil || resp.GetAlterStatus() == nil {
			return merr.CheckRPCCall(nil, err)
		}
		if err := merr.CheckRPCCall(resp.GetAlterStatus(), err); err != nil {
			return err
		}
		c.collCache.Evict(req.GetCollectionName())
		return nil
	})
}

func (c *Client) alterCollectionSchema(ctx context.Context, opt alterCollectionSchemaOption, callOpts ...grpc.CallOption) error {
	if opt == nil {
		return merr.WrapErrParameterMissingMsg("alter collection schema option is nil")
	}
	if err := opt.Validate(); err != nil {
		return err
	}
	return c.alterCollectionSchemaRequest(ctx, opt.Request(), callOpts...)
}

// AddCollectionField adds a field to a collection.
func (c *Client) AddCollectionField(ctx context.Context, opt AddCollectionFieldOption, callOpts ...grpc.CallOption) error {
	if err := opt.Validate(); err != nil {
		return err
	}

	req := opt.Request()
	fieldSchema := &schemapb.FieldSchema{}
	if err := proto.Unmarshal(req.GetSchema(), fieldSchema); err != nil {
		return merr.WrapErrParameterInvalidErr(err, "invalid field schema")
	}

	alterReq := newAlterCollectionSchemaAddRequest(req.GetCollectionName(), fieldSchema, nil)
	alterReq.DbName = req.GetDbName()
	alterReq.CollectionID = req.GetCollectionID()
	err := c.alterCollectionSchemaRequest(ctx, alterReq, callOpts...)
	if grpcstatus.Code(err) != codes.Unimplemented && !errors.Is(err, merr.ErrServiceUnimplemented) {
		return err
	}

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.AddCollectionField(ctx, req, callOpts...)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		c.collCache.Evict(req.GetCollectionName())
		return nil
	})
}

// AddFunctionField adds a function and its output field to a collection.
func (c *Client) AddFunctionField(ctx context.Context, opt AddFunctionFieldOption, callOpts ...grpc.CallOption) error {
	return c.alterCollectionSchema(ctx, opt, callOpts...)
}

// DropCollectionField drops a field from a collection.
func (c *Client) DropCollectionField(ctx context.Context, opt DropCollectionFieldOption, callOpts ...grpc.CallOption) error {
	return c.alterCollectionSchema(ctx, opt, callOpts...)
}

// DropFunctionField drops a function and its output field from a collection.
func (c *Client) DropFunctionField(ctx context.Context, opt DropFunctionFieldOption, callOpts ...grpc.CallOption) error {
	return c.alterCollectionSchema(ctx, opt, callOpts...)
}

// AddCollectionStructField adds a struct array field to a collection.
func (c *Client) AddCollectionStructField(ctx context.Context, opt AddCollectionStructFieldOption, callOpts ...grpc.CallOption) error {
	if err := opt.Validate(); err != nil {
		return err
	}

	req := opt.Request()

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.AddCollectionStructField(ctx, req, callOpts...)
		return merr.CheckRPCCall(resp, err)
	})
	return err
}
