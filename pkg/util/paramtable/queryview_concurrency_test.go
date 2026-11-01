package paramtable

import (
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
)

func TestComponentParam_QueryViewLiveDispatchConcurrency(t *testing.T) {
	params := &ComponentParam{}
	base := NewBaseTable(SkipRemote(true))
	t.Cleanup(base.mgr.Close)
	params.Init(base)
	for _, test := range []struct {
		name string
		key  string
		item *ParamItem
	}{
		{name: "live event dispatch", key: "streaming.queryView.liveEventDispatchConcurrencyRatioPerPChannel", item: &params.StreamingCfg.QueryViewLiveEventDispatchConcurrencyPerPChannel},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.key, test.item.Key)
			require.Equal(t, "0.25", test.item.DefaultValue)
			require.Equal(t, "3.1.0", test.item.Version)
			for _, cpuCount := range []int{1, 3, 4, 8, 10} {
				t.Run(fmt.Sprint(cpuCount), func(t *testing.T) {
					patch := mockey.Mock(hardware.GetCPUNum).Return(cpuCount).Build()
					defer patch.UnPatch()
					require.NoError(t, params.Reset(test.key))
					require.Equal(t, max(1, cpuCount/4), test.item.GetAsInt())
					for _, ratio := range []struct {
						value string
						want  int
					}{
						{value: "0.5", want: max(1, cpuCount/2)},
						{value: "2", want: cpuCount * 2},
						{value: "0", want: 1},
						{value: "-1", want: 1},
					} {
						require.NoError(t, params.Save(test.key, ratio.value))
						require.Equal(t, ratio.want, test.item.GetAsInt())
					}
				})
			}
		})
	}
}
