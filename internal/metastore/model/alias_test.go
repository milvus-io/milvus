package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

func TestAlias_Available(t *testing.T) {
	type fields struct {
		Name         string
		CollectionID int64
		CreatedTime  uint64
		State        etcdpb.AliasState
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			fields: fields{State: etcdpb.AliasState_AliasCreated},
			want:   true,
		},
		{
			fields: fields{State: etcdpb.AliasState_AliasCreating},
			want:   false,
		},

		{
			fields: fields{State: etcdpb.AliasState_AliasDropping},
			want:   false,
		},
		{
			fields: fields{State: etcdpb.AliasState_AliasDropped},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := Alias{
				Name:         tt.fields.Name,
				CollectionID: tt.fields.CollectionID,
				CreatedTime:  tt.fields.CreatedTime,
				State:        tt.fields.State,
			}
			assert.Equalf(t, tt.want, a.Available(), "Available()")
		})
	}
}

func TestAlias_Clone(t *testing.T) {
	type fields struct {
		Name         string
		CollectionID int64
		CreatedTime  uint64
		State        etcdpb.AliasState
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{fields: fields{Name: "alias1", CollectionID: 101}},
		{fields: fields{Name: "alias2", CollectionID: 102}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := Alias{
				Name:         tt.fields.Name,
				CollectionID: tt.fields.CollectionID,
				CreatedTime:  tt.fields.CreatedTime,
				State:        tt.fields.State,
			}
			clone := a.Clone()
			assert.True(t, clone.Equal(a))
		})
	}
}

func TestAlias_Codec(t *testing.T) {
	alias := &Alias{
		Name:         "alias",
		CollectionID: 101,
		CreatedTime:  10000,
		State:        etcdpb.AliasState_AliasCreated,
	}
	aliasPb := MarshalAliasModel(alias)
	aliasFromPb := UnmarshalAliasModel(aliasPb)
	assert.True(t, aliasFromPb.Equal(*alias))
}
