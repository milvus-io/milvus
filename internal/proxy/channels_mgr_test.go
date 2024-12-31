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

package proxy

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_removeDuplicate(t *testing.T) {
	s1 := []string{"11", "11"}
	filtered1 := removeDuplicate(s1)
	assert.ElementsMatch(t, filtered1, []string{"11"})
}

func Test_newChannels(t *testing.T) {
	t.Run("length mismatch", func(t *testing.T) {
		_, err := newChannels([]string{"111", "222"}, []string{"111"})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		got, err := newChannels([]string{"111", "222"}, []string{"111", "111"})
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got.vchans)
		// assert.ElementsMatch(t, []string{"111"}, got.pchans)
		assert.ElementsMatch(t, []string{"111", "111"}, got.pchans)
	})
}

func Test_getDmlChannelsFunc(t *testing.T) {
	t.Run("failed to describe collection", func(t *testing.T) {
		ctx := context.Background()
		rc := newMockRootCoord()
		rc.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return nil, errors.New("mock")
		}
		f := getDmlChannelsFunc(ctx, rc)
		_, err := f(100)
		assert.Error(t, err)
	})

	t.Run("error code not success", func(t *testing.T) {
		ctx := context.Background()
		rc := newMockRootCoord()
		rc.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil
		}
		f := getDmlChannelsFunc(ctx, rc)
		_, err := f(100)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		rc := newMockRootCoord()
		rc.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				VirtualChannelNames:  []string{"111", "222"},
				PhysicalChannelNames: []string{"111", "111"},
				Status:               &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			}, nil
		}
		f := getDmlChannelsFunc(ctx, rc)
		got, err := f(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got.vchans)
		// assert.ElementsMatch(t, []string{"111"}, got.pchans)
		assert.ElementsMatch(t, []string{"111", "111"}, got.pchans)
	})
}

func Test_singleTypeChannelsMgr_getAllChannels(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{
				100: {channelInfos: channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}},
			},
		}
		got, err := m.getAllChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got.vchans)
		assert.ElementsMatch(t, []string{"111"}, got.pchans)
	})

	t.Run("not found", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{},
		}
		_, err := m.getAllChannels(100)
		assert.Error(t, err)
	})
}

func Test_singleTypeChannelsMgr_getPChans(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}, nil
			},
		}
		got, err := m.getPChans(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.getPChans(100)
		assert.Error(t, err)
	})
}

func Test_singleTypeChannelsMgr_getVChans(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}, nil
			},
		}
		got, err := m.getVChans(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.getVChans(100)
		assert.Error(t, err)
	})
}

func Test_singleTypeChannelsMgr_getChannels(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{
				100: {channelInfos: channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}},
			},
		}
		got, err := m.getChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.getChannels(100)
		assert.Error(t, err)
	})
}

func Test_singleTypeChannelsMgr_getVChannels(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{
				100: {channelInfos: channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}},
			},
		}
		got, err := m.getVChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.getVChannels(100)
		assert.Error(t, err)
	})
}

func Test_createStream(t *testing.T) {
	t.Run("failed to create msgstream", func(t *testing.T) {
		factory := newMockMsgStreamFactory()
		factory.fQStream = func(ctx context.Context) (msgstream.MsgStream, error) {
			return nil, errors.New("mock")
		}
		_, err := createStream(context.TODO(), factory, nil, nil)
		assert.Error(t, err)
	})

	t.Run("failed to create query msgstream", func(t *testing.T) {
		factory := newMockMsgStreamFactory()
		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return nil, errors.New("mock")
		}
		_, err := createStream(context.TODO(), factory, nil, nil)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		factory := newMockMsgStreamFactory()
		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return newMockMsgStream(), nil
		}
		_, err := createStream(context.TODO(), factory, []string{"111"}, func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error) {
			return nil, nil
		})
		assert.NoError(t, err)
	})
}

func Test_singleTypeChannelsMgr_createMsgStream(t *testing.T) {
	paramtable.Init()
	t.Run("re-create", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{
				100: {stream: newMockMsgStream()},
			},
		}
		stream, err := m.createMsgStream(context.TODO(), 100)
		assert.NoError(t, err)
		assert.NotNil(t, stream)
	})

	t.Run("concurrent create", func(t *testing.T) {
		factory := newMockMsgStreamFactory()
		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return newMockMsgStream(), nil
		}
		stopCh := make(chan struct{})
		readyCh := make(chan struct{})
		m := &singleTypeChannelsMgr{
			infos: make(map[UniqueID]streamInfos),
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				close(readyCh)
				<-stopCh
				return channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}, nil
			},
			msgStreamFactory: factory,
			repackFunc:       nil,
		}

		firstStream := streamInfos{stream: newMockMsgStream()}
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := m.createMsgStream(context.TODO(), 100)
			assert.NoError(t, err)
			assert.NotNil(t, stream)
		}()
		// make sure create msg stream has run at getchannels
		<-readyCh
		// mock create stream for same collection in same time.
		m.mu.Lock()
		m.infos[100] = firstStream
		m.mu.Unlock()

		close(stopCh)
		wg.Wait()
	})
	t.Run("failed to get channels", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.createMsgStream(context.TODO(), 100)
		assert.Error(t, err)
	})

	t.Run("failed to create message stream", func(t *testing.T) {
		factory := newMockMsgStreamFactory()
		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return nil, errors.New("mock")
		}
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}, nil
			},
			msgStreamFactory: factory,
			repackFunc:       nil,
		}
		_, err := m.createMsgStream(context.TODO(), 100)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		factory := newMockMsgStreamFactory()
		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return newMockMsgStream(), nil
		}
		m := &singleTypeChannelsMgr{
			infos: make(map[UniqueID]streamInfos),
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}, nil
			},
			msgStreamFactory: factory,
			repackFunc:       nil,
		}
		stream, err := m.createMsgStream(context.TODO(), 100)
		assert.NoError(t, err)
		assert.NotNil(t, stream)
		stream, err = m.getOrCreateStream(context.TODO(), 100)
		assert.NoError(t, err)
		assert.NotNil(t, stream)
	})
}

func Test_singleTypeChannelsMgr_lockGetStream(t *testing.T) {
	t.Run("collection not found", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: make(map[UniqueID]streamInfos),
		}
		_, err := m.lockGetStream(100)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{
				100: {stream: newMockMsgStream()},
			},
		}
		stream, err := m.lockGetStream(100)
		assert.NoError(t, err)
		assert.NotNil(t, stream)
	})
}

func Test_singleTypeChannelsMgr_getStream(t *testing.T) {
	t.Run("exist", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{
				100: {stream: newMockMsgStream()},
			},
		}
		stream, err := m.getOrCreateStream(context.TODO(), 100)
		assert.NoError(t, err)
		assert.NotNil(t, stream)
	})

	t.Run("failed to create", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{},
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.getOrCreateStream(context.TODO(), 100)
		assert.Error(t, err)
	})

	t.Run("get after create", func(t *testing.T) {
		factory := newMockMsgStreamFactory()
		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return newMockMsgStream(), nil
		}
		m := &singleTypeChannelsMgr{
			infos: make(map[UniqueID]streamInfos),
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}, nil
			},
			msgStreamFactory: factory,
			repackFunc:       nil,
		}
		stream, err := m.getOrCreateStream(context.TODO(), 100)
		assert.NoError(t, err)
		assert.NotNil(t, stream)
	})
}

func Test_singleTypeChannelsMgr_removeStream(t *testing.T) {
	m := &singleTypeChannelsMgr{
		infos: map[UniqueID]streamInfos{
			100: {
				stream: newMockMsgStream(),
			},
		},
	}
	m.removeStream(100)
	_, err := m.lockGetStream(100)
	assert.Error(t, err)
}

func Test_singleTypeChannelsMgr_removeAllStream(t *testing.T) {
	m := &singleTypeChannelsMgr{
		infos: map[UniqueID]streamInfos{
			100: {
				stream: newMockMsgStream(),
			},
		},
	}
	m.removeAllStream()
	_, err := m.lockGetStream(100)
	assert.Error(t, err)
}
