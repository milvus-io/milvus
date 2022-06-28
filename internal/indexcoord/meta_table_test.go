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

package indexcoord

import (
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMetaTable_NewMetaTable(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		indexMeta := &indexpb.IndexMeta{
			IndexBuildID: 1,
			State:        commonpb.IndexState_Unissued,
		}
		value, err := proto.Marshal(indexMeta)
		assert.NoError(t, err)
		kv := &mockETCDKV{
			loadWithRevisionAndVersions: func(s string) ([]string, []string, []int64, int64, error) {
				return []string{"1"}, []string{string(value)}, []int64{1}, 1, nil
			},
		}
		mt, err := NewMetaTable(kv)
		assert.NoError(t, err)
		assert.NotNil(t, mt)
	})

	t.Run("LoadWithRevisionAndVersions error", func(t *testing.T) {
		kv := &mockETCDKV{
			loadWithRevisionAndVersions: func(s string) ([]string, []string, []int64, int64, error) {
				return nil, nil, nil, 0, errors.New("error")
			},
		}
		mt, err := NewMetaTable(kv)
		assert.Error(t, err)
		assert.Nil(t, mt)
	})

	t.Run("Unmarshal error", func(t *testing.T) {
		kv := &mockETCDKV{
			loadWithRevisionAndVersions: func(s string) ([]string, []string, []int64, int64, error) {
				return []string{"1"}, []string{"invalid_string"}, []int64{1}, 1, nil
			},
		}
		mt, err := NewMetaTable(kv)
		assert.Error(t, err)
		assert.Nil(t, mt)
	})
}

func TestMetaTable_GetAllIndexMeta(t *testing.T) {
	mt := metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 1,
					State:        commonpb.IndexState_Unissued,
				},
			},
			2: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 2,
					State:        commonpb.IndexState_Finished,
					MarkDeleted:  true,
				},
			},
		},
	}

	metas := mt.GetAllIndexMeta()
	assert.Equal(t, 2, len(metas))
}

func TestMetaTable_AddIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := &metaTable{
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
			indexBuildID2Meta: make(map[int64]*Meta),
		}

		req := &indexpb.BuildIndexRequest{
			IndexID: 1,
		}
		err := mt.AddIndex(1, req)
		assert.NoError(t, err)

		err = mt.AddIndex(1, req)
		assert.NoError(t, err)
	})

	t.Run("save meta fail", func(t *testing.T) {
		mt := &metaTable{
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, errors.New("error")
				},
			},
			indexBuildID2Meta: make(map[int64]*Meta),
		}

		req := &indexpb.BuildIndexRequest{
			IndexID: 1,
		}
		err := mt.AddIndex(1, req)
		assert.Error(t, err)
	})
}

func TestMetaTable_ResetNodeID(t *testing.T) {
	mt := metaTable{
		client: &mockETCDKV{
			compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
				return true, nil
			},
		},
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 1,
					NodeID:       2,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		err := mt.ResetNodeID(1)
		assert.NoError(t, err)
	})

	t.Run("index not exist", func(t *testing.T) {
		err := mt.ResetNodeID(2)
		assert.Error(t, err)
	})

	t.Run("save meta fail, reload fail", func(t *testing.T) {
		mk := &mockETCDKV{
			compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
				return true, errors.New("error occurred")
			},
			loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
				return nil, nil, nil, errors.New("error occurred")
			},
		}
		mt.client = mk
		err := mt.ResetNodeID(1)
		assert.Error(t, err)
	})

	t.Run("save meta fail", func(t *testing.T) {
		mk := &mockETCDKV{
			compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
				return true, errors.New("error occurred")
			},
			loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
				v, _ := proto.Marshal(&indexpb.IndexMeta{
					IndexBuildID: 1,
					NodeID:       1,
				})
				return nil, []string{string(v)}, []int64{1}, nil
			},
		}
		mt.client = mk
		err := mt.ResetNodeID(1)
		assert.Error(t, err)
	})

	t.Run("compare version fail", func(t *testing.T) {
		mk := &mockETCDKV{
			compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
				return false, nil
			},
			loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
				v, _ := proto.Marshal(&indexpb.IndexMeta{
					IndexBuildID: 1,
					NodeID:       1,
				})
				return nil, []string{string(v)}, []int64{1}, nil
			},
		}
		mt.client = mk
		err := mt.ResetNodeID(1)
		assert.Error(t, err)
	})
}

func TestMetaTable_GetMeta(t *testing.T) {
	mt := metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 1,
					NodeID:       2,
				},
			},
		},
	}

	meta, ok := mt.GetMeta(1)
	assert.True(t, ok)
	assert.Equal(t, int64(1), meta.indexMeta.IndexBuildID)
}

func TestMetaTable_UpdateVersion(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       0,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
		}
		err := mt.UpdateVersion(1, 1)
		assert.NoError(t, err)
	})

	t.Run("index meta not exist", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       0,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return false, nil
				},
			},
		}
		err := mt.UpdateVersion(2, 1)
		assert.Error(t, err)
	})

	t.Run("can not index", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       0,
						State:        commonpb.IndexState_Unissued,
						MarkDeleted:  true,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
		}
		err := mt.UpdateVersion(1, 1)
		assert.Error(t, err)

		mt = metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						State:        commonpb.IndexState_InProgress,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
		}
		err = mt.UpdateVersion(1, 2)
		assert.Error(t, err)

		mt = metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       0,
						State:        commonpb.IndexState_Finished,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
		}
		err = mt.UpdateVersion(1, 2)
		assert.Error(t, err)
	})

	t.Run("save meta fail", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       0,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, errors.New("error")
				},
			},
		}

		err := mt.UpdateVersion(1, 1)
		assert.Error(t, err)
	})

	t.Run("compare version fail load success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       0,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return false, nil
				},
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					indexMeta := &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       0,
						State:        commonpb.IndexState_Unissued,
					}
					v, _ := proto.Marshal(indexMeta)
					return []string{"1"}, []string{string(v)}, []int64{1}, nil
				},
			},
		}

		err := mt.UpdateVersion(1, 1)
		assert.Error(t, err)
	})

	t.Run("compare version fail load fail", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       0,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return false, nil
				},
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					return nil, nil, nil, errors.New("error")
				},
			},
		}

		err := mt.UpdateVersion(1, 1)
		assert.Error(t, err)
	})
}

func TestMetaTable_BuildIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
		}

		err := mt.BuildIndex(1)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.IndexState_InProgress, mt.indexBuildID2Meta[1].indexMeta.State)
	})

	t.Run("index meta not exist", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
		}

		err := mt.BuildIndex(2)
		assert.Error(t, err)
	})

	t.Run("index meta marked deleted", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						State:        commonpb.IndexState_Unissued,
						MarkDeleted:  true,
					},
				},
			},
		}

		err := mt.BuildIndex(2)
		assert.Error(t, err)
	})

	t.Run("save meta fail", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, errors.New("error")
				},
			},
		}

		err := mt.BuildIndex(1)
		assert.Error(t, err)
	})

	t.Run("compare version fail load success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return false, nil
				},
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					indexMeta := &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						State:        commonpb.IndexState_Unissued,
					}
					v, _ := proto.Marshal(indexMeta)
					return []string{"1"}, []string{string(v)}, []int64{1}, nil
				},
			},
		}

		err := mt.BuildIndex(1)
		assert.Error(t, err)
	})

	t.Run("compare version fail load fail", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						State:        commonpb.IndexState_Unissued,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return false, nil
				},
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					return nil, nil, nil, errors.New("error")
				},
			},
		}

		err := mt.BuildIndex(1)
		assert.Error(t, err)
	})
}

func TestMetaTable_GetMetasByNodeID(t *testing.T) {
	mt := metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 1,
					NodeID:       1,
					State:        commonpb.IndexState_Unissued,
					MarkDeleted:  true,
				},
			},
			2: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 2,
					NodeID:       1,
					State:        commonpb.IndexState_Finished,
				},
			},
		},
	}
	metas := mt.GetMetasByNodeID(1)
	assert.Equal(t, 1, len(metas))
}

func TestMetaTable_MarkIndexAsDeleted(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State:       commonpb.IndexState_Unissued,
						MarkDeleted: true,
					},
				},
				2: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 2,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Finished,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
		}

		buildIDs, err := mt.MarkIndexAsDeleted(1)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(buildIDs))
	})

	t.Run("save fail", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State:       commonpb.IndexState_Unissued,
						MarkDeleted: true,
					},
				},
				2: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 2,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Finished,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, errors.New("error")
				},
			},
		}

		buildIDs, err := mt.MarkIndexAsDeleted(1)
		assert.Error(t, err)
		assert.Equal(t, 0, len(buildIDs))
	})
	t.Run("compare version fail load success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State:       commonpb.IndexState_Unissued,
						MarkDeleted: true,
					},
				},
				2: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 2,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Finished,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return false, nil
				},
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					indexMeta := &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Unissued,
					}
					v, err := proto.Marshal(indexMeta)
					return []string{"1"}, []string{string(v)}, []int64{1}, err
				},
			},
		}

		buildIDs, err := mt.MarkIndexAsDeleted(1)
		assert.Error(t, err)
		assert.Equal(t, 0, len(buildIDs))
	})

	t.Run("compare version fail load fail", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State:       commonpb.IndexState_Unissued,
						MarkDeleted: true,
					},
				},
				2: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 2,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Finished,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return false, nil
				},
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					return []string{}, []string{}, []int64{}, errors.New("error")
				},
			},
		}

		buildIDs, err := mt.MarkIndexAsDeleted(1)
		assert.Error(t, err)
		assert.Equal(t, 0, len(buildIDs))
	})
}

func TestMetaTable_MarkIndexAsDeletedByBuildIDs(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State:       commonpb.IndexState_Unissued,
						MarkDeleted: true,
					},
				},
				2: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 2,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Finished,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
		}

		err := mt.MarkIndexAsDeletedByBuildIDs([]UniqueID{1, 2})
		assert.NoError(t, err)
	})

	t.Run("save fail", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State:       commonpb.IndexState_Unissued,
						MarkDeleted: true,
					},
				},
				2: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 2,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Finished,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, errors.New("error")
				},
			},
		}

		err := mt.MarkIndexAsDeletedByBuildIDs([]UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("compare version fail load success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State:       commonpb.IndexState_Unissued,
						MarkDeleted: true,
					},
				},
				2: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 2,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Finished,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return false, nil
				},
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					indexMeta := &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Unissued,
					}
					v, err := proto.Marshal(indexMeta)
					return []string{"1"}, []string{string(v)}, []int64{1}, err
				},
			},
		}

		err := mt.MarkIndexAsDeletedByBuildIDs([]UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("compare fail load fail", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State:       commonpb.IndexState_Unissued,
						MarkDeleted: true,
					},
				},
				2: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 2,
						NodeID:       1,
						Req: &indexpb.BuildIndexRequest{
							IndexID: 1,
						},
						State: commonpb.IndexState_Finished,
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return false, nil
				},
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					return []string{}, []string{}, []int64{}, errors.New("error")
				},
			},
		}

		err := mt.MarkIndexAsDeletedByBuildIDs([]UniqueID{1, 2})
		assert.Error(t, err)
	})
}

func TestMetaTable_GetIndexStates(t *testing.T) {
	mt := metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 1,
					IndexVersion: 1,
					NodeID:       1,
					State:        commonpb.IndexState_Unissued,
					Req: &indexpb.BuildIndexRequest{
						IndexID: 1,
					},
				},
			},
			2: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 2,
					IndexVersion: 2,
					NodeID:       2,
					State:        commonpb.IndexState_InProgress,
					Req: &indexpb.BuildIndexRequest{
						IndexID: 1,
					},
				},
			},
			3: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 3,
					IndexVersion: 2,
					NodeID:       3,
					State:        commonpb.IndexState_Finished,
					MarkDeleted:  true,
					Req: &indexpb.BuildIndexRequest{
						IndexID: 1,
					},
				},
			},
		},
	}

	states := mt.GetIndexStates([]UniqueID{1, 2, 3, 4})
	assert.Equal(t, 4, len(states))
}

func TestMetaTable_GetIndexFilePathInfo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID:   1,
						IndexVersion:   1,
						NodeID:         1,
						State:          commonpb.IndexState_Finished,
						IndexFilePaths: []string{"file1", "file2", "file3"},
					},
				},
			},
		}
		info, err := mt.GetIndexFilePathInfo(1)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"file1", "file2", "file3"}, info.IndexFilePaths)
	})

	t.Run("index meta not exist", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID:   1,
						IndexVersion:   1,
						NodeID:         1,
						State:          commonpb.IndexState_Finished,
						IndexFilePaths: []string{"file1", "file2", "file3"},
					},
				},
			},
		}
		info, err := mt.GetIndexFilePathInfo(2)
		assert.Error(t, err)
		assert.Nil(t, info)
	})

	t.Run("index meta deleted", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID:   1,
						IndexVersion:   1,
						NodeID:         1,
						State:          commonpb.IndexState_Finished,
						IndexFilePaths: []string{"file1", "file2", "file3"},
						MarkDeleted:    true,
					},
				},
			},
		}
		info, err := mt.GetIndexFilePathInfo(1)
		assert.Error(t, err)
		assert.Nil(t, info)
	})

	t.Run("index meta not finished", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						IndexVersion: 1,
						NodeID:       1,
						State:        commonpb.IndexState_InProgress,
					},
				},
			},
		}
		info, err := mt.GetIndexFilePathInfo(1)
		assert.Error(t, err)
		assert.Nil(t, info)
	})
}

func TestMetaTable_DeleteIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						IndexVersion: 1,
						NodeID:       1,
						State:        commonpb.IndexState_InProgress,
					},
				},
			},
			client: &mockETCDKV{
				remove: func(s string) error {
					return nil
				},
			},
		}
		err := mt.DeleteIndex(1)
		assert.NoError(t, err)
	})

	t.Run("remove meta fail", func(t *testing.T) {
		mt := metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						IndexVersion: 1,
						NodeID:       1,
						State:        commonpb.IndexState_InProgress,
					},
				},
			},
			client: &mockETCDKV{
				remove: func(s string) error {
					return errors.New("error")
				},
			},
		}
		err := mt.DeleteIndex(1)
		assert.Error(t, err)
	})
}

func TestMetaTable_GetBuildID2IndexFiles(t *testing.T) {
	mt := metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexFilePaths: []string{"file1", "file2"},
				},
			},
		},
	}

	indexFiles := mt.GetBuildID2IndexFiles()
	assert.Equal(t, 1, len(indexFiles))
	assert.ElementsMatch(t, []string{"file1", "file2"}, indexFiles[1])
}

func TestMetaTable_GetDeletedMetas(t *testing.T) {
	mt := metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					MarkDeleted: true,
				},
			},
			2: {
				indexMeta: &indexpb.IndexMeta{
					MarkDeleted: true,
				},
			},
		},
	}
	deletedMeta := mt.GetDeletedMetas()
	assert.Equal(t, 2, len(deletedMeta))
}

func TestMetaTable_HasSameReq(t *testing.T) {
	mt := metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 1,
					Req: &indexpb.BuildIndexRequest{
						IndexID:   1,
						IndexName: "index-1",
						DataPaths: []string{"file1", "file2"},
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
						IndexParams: []*commonpb.KeyValuePair{
							{
								Key:   "metric_type",
								Value: "L2",
							},
						},
						SegmentID: 2,
					},
					MarkDeleted: true,
				},
			},
		},
	}

	req := &indexpb.BuildIndexRequest{
		SegmentID: 3,
	}
	exist, buildID := mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Equal(t, int64(0), buildID)

	req = &indexpb.BuildIndexRequest{
		SegmentID: 2,
		IndexID:   2,
	}
	exist, buildID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Equal(t, int64(0), buildID)

	req = &indexpb.BuildIndexRequest{
		SegmentID: 2,
		IndexID:   1,
		IndexName: "index-2",
	}
	exist, buildID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Equal(t, int64(0), buildID)

	req = &indexpb.BuildIndexRequest{
		SegmentID: 2,
		IndexID:   1,
		IndexName: "index-1",
		DataPaths: []string{"file1", "file2"},
	}
	exist, buildID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Equal(t, int64(0), buildID)

	req = &indexpb.BuildIndexRequest{
		SegmentID: 2,
		IndexID:   1,
		IndexName: "index-1",
		DataPaths: []string{"file1", "file2"},
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "128",
			},
			{
				Key:   "param",
				Value: "param",
			},
		},
	}
	exist, buildID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Equal(t, int64(0), buildID)

	req = &indexpb.BuildIndexRequest{
		SegmentID: 2,
		IndexID:   1,
		IndexName: "index-1",
		DataPaths: []string{"file1", "file2"},
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "256",
			},
		},
	}
	exist, buildID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Equal(t, int64(0), buildID)

	req = &indexpb.BuildIndexRequest{
		SegmentID: 2,
		IndexID:   1,
		IndexName: "index-1",
		DataPaths: []string{"file1", "file2"},
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "128",
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "metric_type",
				Value: "L2",
			},
			{
				Key:   "dim",
				Value: "128",
			},
		},
	}
	exist, buildID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Equal(t, int64(0), buildID)

	req = &indexpb.BuildIndexRequest{
		SegmentID: 2,
		IndexID:   1,
		IndexName: "index-1",
		DataPaths: []string{"file1", "file2"},
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "128",
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "metric_type",
				Value: "IP",
			},
		},
	}
	exist, buildID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Equal(t, int64(0), buildID)

	req = &indexpb.BuildIndexRequest{
		SegmentID: 2,
		IndexID:   1,
		IndexName: "index-1",
		DataPaths: []string{"file1", "file2"},
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "128",
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "metric_type",
				Value: "L2",
			},
		},
	}
	exist, buildID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Equal(t, int64(0), buildID)

	mt = metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 1,
					Req: &indexpb.BuildIndexRequest{
						IndexID:   1,
						IndexName: "index-1",
						DataPaths: []string{"file1", "file2"},
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
						IndexParams: []*commonpb.KeyValuePair{
							{
								Key:   "metric_type",
								Value: "L2",
							},
						},
						SegmentID: 2,
					},
				},
			},
		},
	}
	req = &indexpb.BuildIndexRequest{
		SegmentID: 2,
		IndexID:   1,
		IndexName: "index-1",
		DataPaths: []string{"file1", "file2"},
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "128",
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "metric_type",
				Value: "L2",
			},
		},
	}
	exist, buildID = mt.HasSameReq(req)
	assert.True(t, exist)
	assert.Equal(t, int64(1), buildID)
}

func TestMetaTable_NeedUpdateMeta(t *testing.T) {
	mt := metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 1,
					State:        commonpb.IndexState_Unissued,
				},
				etcdVersion: 1,
			},
		},
	}

	t.Run("index not exist", func(t *testing.T) {
		meta := &Meta{
			indexMeta: &indexpb.IndexMeta{
				IndexBuildID: 2,
				State:        commonpb.IndexState_Finished,
			},
		}
		update := mt.NeedUpdateMeta(meta)
		assert.False(t, update)
	})

	t.Run("update", func(t *testing.T) {
		meta := &Meta{
			indexMeta: &indexpb.IndexMeta{
				IndexBuildID: 1,
				State:        commonpb.IndexState_Finished,
			},
			etcdVersion: 2,
		}
		update := mt.NeedUpdateMeta(meta)
		assert.True(t, update)

		update = mt.NeedUpdateMeta(meta)
		assert.False(t, update)
	})
}
