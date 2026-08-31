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

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// manualMode puts the cluster in the mode where the user owns the shard count:
// the feature on, the size trigger off. The two are exclusive, so every test of
// the downstream validations has to select it first.
func manualMode(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.ShardSplitEnable.Key, "true")
	pt.Save(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key, "false")
	t.Cleanup(func() {
		pt.Reset(pt.DataCoordCfg.ShardSplitEnable.Key)
		pt.Reset(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key)
	})
}

// autoMode puts the cluster in the other mode: the size trigger owns the shard
// count, so setting one by hand is refused.
func autoMode(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.ShardSplitEnable.Key, "true")
	pt.Save(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key, "true")
	t.Cleanup(func() {
		pt.Reset(pt.DataCoordCfg.ShardSplitEnable.Key)
		pt.Reset(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key)
	})
}

func shardNumProp(value string) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{{Key: common.CollectionShardNum, Value: value}}
}

func hashCollection(shards int) *model.Collection {
	coll := &model.Collection{Name: "col"}
	for i := 0; i < shards; i++ {
		coll.VirtualChannelNames = append(coll.VirtualChannelNames, string(rune('a'+i)))
	}
	return coll
}

func TestValidateDesiredShardNumAcceptsAGrowth(t *testing.T) {
	manualMode(t)
	assert.NoError(t, validateDesiredShardNum(hashCollection(3), shardNumProp("8"), nil))
}

func TestValidateDesiredShardNumIgnoresUnrelatedProperties(t *testing.T) {
	// Every other AlterCollection must pass through untouched.
	props := []*commonpb.KeyValuePair{{Key: common.CollectionTTLConfigKey, Value: "60"}}
	assert.NoError(t, validateDesiredShardNum(hashCollection(3), props, nil))
	assert.NoError(t, validateDesiredShardNum(nil, props, nil))
}

func TestValidateDesiredShardNumAcceptsAWithdrawal(t *testing.T) {
	manualMode(t)
	// Deleting the property withdraws the request. It is how a rehash is
	// cancelled, so it must not be refused for any of the reasons a SET is.
	assert.NoError(t, validateDesiredShardNum(hashCollection(3),
		nil, []string{common.CollectionShardNum}))

	// Deletes are applied after sets by the caller, so a request that does both
	// ends up withdrawing — validating the set would refuse a request that in
	// fact asks for nothing. Shrinking is used here because it is refused as a
	// set, which makes the ordering observable.
	assert.NoError(t, validateDesiredShardNum(hashCollection(8),
		shardNumProp("4"), []string{common.CollectionShardNum}))
}

func TestValidateDesiredShardNumWithdrawalIgnoresTheClusterMode(t *testing.T) {
	// Automatic mode refuses a shard count to be SET by hand. Withdrawing one
	// left over from before the switch must stay possible, or the stale property
	// could never be cleaned up.
	autoMode(t)
	assert.NoError(t, validateDesiredShardNum(hashCollection(3),
		nil, []string{common.CollectionShardNum}))
	require.Error(t, validateDesiredShardNum(hashCollection(3), shardNumProp("8"), nil))
}

// Shrinking passes rootcoord validation now. The checks it could not make --
// whether the survivors can hold the merge -- need the collection's row and byte
// counts, which live in datacoord; see TestCheckProjectedShardLoad.
func TestValidateDesiredShardNumAcceptsShrinking(t *testing.T) {
	manualMode(t)
	assert.NoError(t, validateDesiredShardNum(hashCollection(8), shardNumProp("4"), nil))
	assert.NoError(t, validateDesiredShardNum(hashCollection(8), shardNumProp("2"), nil))
}

func TestValidateDesiredShardNumRejections(t *testing.T) {
	manualMode(t)
	cases := []struct {
		name   string
		coll   *model.Collection
		props  []*commonpb.KeyValuePair
		delete []string
		errStr string
	}{
		{
			name:   "not a number",
			coll:   hashCollection(3),
			props:  shardNumProp("many"),
			errStr: "invalid syntax",
		},
		{
			name:   "below two",
			coll:   hashCollection(3),
			props:  shardNumProp("1"),
			errStr: "at least 2",
		},
		{
			// The sources are not retired until their data is rewritten, so the
			// collection needs a pchannel for each source AND each target.
			name:   "not enough pchannels for sources plus targets",
			coll:   hashCollection(10),
			props:  shardNumProp("12"),
			errStr: "raise rootCoord.dmlChannelNum",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDesiredShardNum(tc.coll, tc.props, tc.delete)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errStr)
		})
	}
}

func TestRoutableShardCountSkipsRetiredShards(t *testing.T) {
	// A shard an earlier split retired still occupies a vchannel but owns no key
	// range, so the desired count must be compared against the live shards only.
	coll := &model.Collection{
		Name:                "col",
		VirtualChannelNames: []string{"live", "fenced", "dropped", "creating"},
		ShardInfos: map[string]*model.ShardInfo{
			"live":     {State: schemapb.ShardState_ShardNormal},
			"fenced":   {State: schemapb.ShardState_ShardSplitting},
			"dropped":  {State: schemapb.ShardState_ShardDropped},
			"creating": {State: schemapb.ShardState_ShardCreating},
		},
	}
	assert.Equal(t, int32(2), routableShardCount(coll))

	// Legacy meta with no shard info at all: every vchannel is a live shard.
	assert.Equal(t, int32(3), routableShardCount(hashCollection(3)))
}

func TestValidateDesiredShardNumHonorsTheSystemShardLimit(t *testing.T) {
	manualMode(t)
	// CreateCollection refuses a shard num above proxy.maxShardNum. AlterCollection
	// must refuse it too, or the same limit would depend on which door you came
	// through.
	paramtable.Get().Save(paramtable.Get().ProxyCfg.MaxShardNum.Key, "4")
	defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.MaxShardNum.Key)

	err := validateDesiredShardNum(hashCollection(2), shardNumProp("5"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds system limit")

	assert.NoError(t, validateDesiredShardNum(hashCollection(2), shardNumProp("4"), nil))
}

func TestAvailablePChannelCountFollowsTheActiveMode(t *testing.T) {
	pt := paramtable.Get()

	// Default: the set is generated from dmlChannelNum, and raising it applies
	// online.
	pt.Save(pt.CommonCfg.PreCreatedTopicEnabled.Key, "false")
	defer pt.Reset(pt.CommonCfg.PreCreatedTopicEnabled.Key)
	pt.Save(pt.RootCoordCfg.DmlChannelNum.Key, "24")
	defer pt.Reset(pt.RootCoordCfg.DmlChannelNum.Key)
	count, source := availablePChannelCount()
	assert.Equal(t, 24, count)
	assert.Equal(t, "rootCoord.dmlChannelNum", source)

	// Pre-created topics: the set is the explicit list, and dmlChannelNum says
	// nothing about it. Reporting the wrong key here would send an operator to
	// change a setting that has no effect in this mode.
	pt.Save(pt.CommonCfg.PreCreatedTopicEnabled.Key, "true")
	pt.Save(pt.CommonCfg.TopicNames.Key, "t0,t1,t2")
	defer pt.Reset(pt.CommonCfg.TopicNames.Key)
	count, source = availablePChannelCount()
	assert.Equal(t, 3, count)
	assert.Equal(t, "common.topicNames", source)
}

func TestValidateDesiredShardNumRefusedInAutomaticMode(t *testing.T) {
	// The two ways of sizing shards are exclusive: with the size trigger on, a
	// user request must be REFUSED here rather than recorded and then silently
	// ignored by the reconciler — otherwise the caller sees success and nothing
	// ever happens.
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.ShardSplitEnable.Key, "true")
	pt.Save(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key, "true")
	defer pt.Reset(pt.DataCoordCfg.ShardSplitEnable.Key)
	defer pt.Reset(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key)

	// A collection that has not chosen a mode is automatic, so a hand-set count
	// is refused -- and the message names the property that takes control, since
	// that is now a per-COLLECTION choice rather than a cluster-wide switch.
	err := validateDesiredShardNum(hashCollection(2), shardNumProp("4"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sizes its shards automatically")
	assert.Contains(t, err.Error(), common.CollectionShardSplitMode,
		"the message must name the property the operator has to set")

	// The collection next to it, in manual mode, is accepted at the same moment
	// with the same cluster settings. That is the whole point of the property.
	manual := hashCollection(2)
	manual.Properties = append(manual.Properties, &commonpb.KeyValuePair{
		Key: common.CollectionShardSplitMode, Value: common.ShardSplitModeManual,
	})
	assert.NoError(t, validateDesiredShardNum(manual, shardNumProp("4"), nil))

	// And one request may do both: switch to manual AND set the count. Refusing
	// that would make taking control always cost two round trips, the first of
	// which fails for the state the second fixes.
	assert.NoError(t, validateDesiredShardNum(hashCollection(2), []*commonpb.KeyValuePair{
		{Key: common.CollectionShardSplitMode, Value: common.ShardSplitModeManual},
		{Key: common.CollectionShardNum, Value: "4"},
	}, nil))
}

func TestValidateShardSplitMode(t *testing.T) {
	assert.NoError(t, validateShardSplitMode(nil))
	for _, v := range []string{"auto", "manual", "MANUAL", " auto "} {
		assert.NoError(t, validateShardSplitMode([]*commonpb.KeyValuePair{
			{Key: common.CollectionShardSplitMode, Value: v},
		}), v)
	}
	// A typo must not read as the "auto" default: that would hand the trigger a
	// collection the operator believes they control.
	err := validateShardSplitMode([]*commonpb.KeyValuePair{
		{Key: common.CollectionShardSplitMode, Value: "manul"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be")
}

func TestValidateDesiredShardNumRefusedWhenTheFeatureIsOff(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.ShardSplitEnable.Key, "false")
	defer pt.Reset(pt.DataCoordCfg.ShardSplitEnable.Key)

	err := validateDesiredShardNum(hashCollection(2), shardNumProp("4"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard split is disabled")
}

func TestValidateDesiredShardNumIgnoresTheModeWhenNoShardNumIsSet(t *testing.T) {
	// An unrelated AlterCollection must pass through even in automatic mode.
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key, "true")
	defer pt.Reset(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key)

	props := []*commonpb.KeyValuePair{{Key: common.CollectionTTLConfigKey, Value: "60"}}
	assert.NoError(t, validateDesiredShardNum(hashCollection(2), props, nil))
}

// TestValidateDesiredShardNumIsIdempotent covers re-applying a declaration the
// collection already satisfies. The property is declarative, so this must be
// accepted rather than refused: a client retrying after a timeout, or an
// operator re-applying a whole config, would otherwise get a hard error for
// asking for the state that already holds. The window is reached in normal
// operation too, since the routable count catches up to the desired value at
// the write switch, long before the rehash finishes.
func TestValidateDesiredShardNumIsIdempotent(t *testing.T) {
	manualMode(t)

	require.NoError(t, validateDesiredShardNum(hashCollection(4), shardNumProp("4"), nil))

	// Mid-rehash: the two sources are fenced and four targets are up, so the
	// routable count already reads 4 while three of the six vchannels are still
	// being rewritten.
	coll := &model.Collection{
		Name:                "c",
		VirtualChannelNames: []string{"v0", "v1", "v2", "v3", "v4", "v5"},
		ShardInfos: map[string]*model.ShardInfo{
			"v0": {State: schemapb.ShardState_ShardSplitting},
			"v1": {State: schemapb.ShardState_ShardSplitting},
			"v2": {State: schemapb.ShardState_ShardCreating},
			"v3": {State: schemapb.ShardState_ShardCreating},
			"v4": {State: schemapb.ShardState_ShardCreating},
			"v5": {State: schemapb.ShardState_ShardCreating},
		},
	}
	require.EqualValues(t, 4, routableShardCount(coll))
	require.NoError(t, validateDesiredShardNum(coll, shardNumProp("4"), nil))
}

// TestCreateCollectionRejectsShardSplitProperties covers the create path.
//
// ParseShardSplitMode exists so a typo cannot silently mean "auto", but the
// check used to hang off AlterCollection only -- so a collection could be
// CREATED with an unrecognised mode, which then read back as the auto default
// and handed the size trigger a collection the operator believed they had taken
// manual control of. Nothing would ever report it.
func TestCreateCollectionRejectsShardSplitProperties(t *testing.T) {
	newTask := func(props []*commonpb.KeyValuePair) *createCollectionTask {
		return &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				CollectionName: "c",
				ShardsNum:      2,
				Properties:     props,
			},
			header: &message.CreateCollectionMessageHeader{},
		}
	}

	err := newTask([]*commonpb.KeyValuePair{
		{Key: common.CollectionShardSplitMode, Value: "manaul"},
	}).validate(context.Background())
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), common.CollectionShardSplitMode)

	// A desired count at create time asks datacoord to rehash a collection that
	// has not taken a write yet; shards_num already says what to create.
	err = newTask(shardNumProp("8")).validate(context.Background())
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), common.CollectionShardNum)

	// A recognised mode is not what these two reject; that it passes the property
	// checks and reaches the capacity ones is covered by TestValidateShardSplitMode
	// and by the create-collection suite.
}
