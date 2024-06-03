package common

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestIsSystemField(t *testing.T) {
	type args struct {
		fieldID int64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{fieldID: StartOfUserFieldID},
			want: false,
		},
		{
			args: args{fieldID: StartOfUserFieldID + 1},
			want: false,
		},
		{
			args: args{fieldID: TimeStampField},
			want: true,
		},
		{
			args: args{fieldID: RowIDField},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsSystemField(tt.args.fieldID), "IsSystemField(%v)", tt.args.fieldID)
		})
	}
}

func TestDatabaseProperties(t *testing.T) {
	props := []*commonpb.KeyValuePair{
		{
			Key:   DatabaseReplicaNumber,
			Value: "3",
		},
		{
			Key:   DatabaseResourceGroups,
			Value: strings.Join([]string{"rg1", "rg2"}, ","),
		},
	}

	replicaNum, err := DatabaseLevelReplicaNumber(props)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), replicaNum)

	rgs, err := DatabaseLevelResourceGroups(props)
	assert.NoError(t, err)
	assert.Contains(t, rgs, "rg1")
	assert.Contains(t, rgs, "rg2")

	// test prop not found
	_, err = DatabaseLevelReplicaNumber(nil)
	assert.Error(t, err)

	_, err = DatabaseLevelResourceGroups(nil)
	assert.Error(t, err)

	// test invalid prop value

	props = []*commonpb.KeyValuePair{
		{
			Key:   DatabaseReplicaNumber,
			Value: "xxxx",
		},
		{
			Key:   DatabaseResourceGroups,
			Value: "",
		},
	}
	_, err = DatabaseLevelReplicaNumber(props)
	assert.Error(t, err)

	_, err = DatabaseLevelResourceGroups(props)
	assert.Error(t, err)
}
