package model

import (
	"testing"
)

func TestCheckPartitionsEqual(t *testing.T) {
	type args struct {
		partitionsA []*Partition
		partitionsB []*Partition
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			// length not match.
			args: args{
				partitionsA: []*Partition{{PartitionName: "_default"}},
				partitionsB: []*Partition{},
			},
			want: false,
		},
		{
			args: args{
				partitionsA: []*Partition{{PartitionName: "_default"}},
				partitionsB: []*Partition{{PartitionName: "not_default"}},
			},
			want: false,
		},
		{
			args: args{
				partitionsA: []*Partition{{PartitionName: "_default"}, {PartitionName: "not_default"}},
				partitionsB: []*Partition{{PartitionName: "_default"}, {PartitionName: "not_default"}},
			},
			want: true,
		},
		{
			args: args{
				partitionsA: []*Partition{{PartitionName: "not_default"}, {PartitionName: "_default"}},
				partitionsB: []*Partition{{PartitionName: "_default"}, {PartitionName: "not_default"}},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckPartitionsEqual(tt.args.partitionsA, tt.args.partitionsB); got != tt.want {
				t.Errorf("CheckPartitionsEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
