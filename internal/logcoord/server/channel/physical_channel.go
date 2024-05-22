package channel

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/logpb"
)

var _ PhysicalChannel = (*physicalChannelImpl)(nil)

type PhysicalChannel interface {
	// Name returns the name of the channel.
	Name() string

	// Term returns the term of the channel.
	Term() int64

	// PChannelInfo returns the info of the channel.
	Info() *logpb.PChannelInfo

	// IsDrop returns whether the channel is dropped.
	IsDrop() bool

	// Assign allocates a new term for the channel, update assigned serverID and return the term.
	Assign(ctx context.Context, serverID int64) (*logpb.PChannelInfo, error)

	// AddVChannel adds a new VChannel to PChannel.
	AddVChannel(ctx context.Context, vChannels ...string) error

	// RemoveVChannel removes a VChannel from PChannel.
	RemoveVChannel(ctx context.Context, vChannels ...string) error

	// Return whether the channel is in used (if vChannel on it).
	IsUsed() bool

	// Drop the channel.
	Drop(ctx context.Context) error
}

// NewPhysicalChannel creates a new PhysicalChannel.
func NewPhysicalChannel(catalog metastore.LogCoordCataLog, info *logpb.PChannelInfo) PhysicalChannel {
	return &physicalChannelImpl{
		catalog: catalog,
		mu:      &sync.RWMutex{},
		info:    info,
		dropped: false,
	}
}

type physicalChannelImpl struct {
	catalog metastore.LogCoordCataLog
	mu      *sync.RWMutex
	info    *logpb.PChannelInfo
	dropped bool
}

// Name returns the name of the channel.
func (pc *physicalChannelImpl) Name() string {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.info.Name
}

// Term returns the term of the channel.
func (pc *physicalChannelImpl) Term() int64 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.info.Term
}

// IsUsed returns whether the channel is in used (if vChannel on it).
func (pc *physicalChannelImpl) IsUsed() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if pc.dropped {
		return false
	}
	return len(pc.info.VChannelInfos) > 0
}

// IsDrop returns whether the channel is dropped.
func (pc *physicalChannelImpl) IsDrop() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.dropped
}

// Info returns the copy of the channel info.
func (pc *physicalChannelImpl) Info() *logpb.PChannelInfo {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return proto.Clone(pc.info).(*logpb.PChannelInfo)
}

// Assign allocates a new term for the channel, update assigned serverID and return the term.
func (pc *physicalChannelImpl) Assign(ctx context.Context, serverID int64) (*logpb.PChannelInfo, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.dropped {
		return nil, ErrNotExists
	}

	// Allocate new term.
	// Just increase the term by 1 and save.
	newChannel := proto.Clone(pc.info).(*logpb.PChannelInfo)
	newChannel.Term++
	newChannel.ServerID = serverID
	if err := pc.catalog.SavePChannel(ctx, newChannel); err != nil {
		return nil, err
	}
	pc.info = newChannel
	return proto.Clone(newChannel).(*logpb.PChannelInfo), nil
}

// AddVChannel adds a new VChannel to PChannel.
func (pc *physicalChannelImpl) AddVChannel(ctx context.Context, vChannels ...string) error {
	if len(vChannels) == 0 {
		return nil
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.dropped {
		return ErrDropped
	}

	// should we report error if vchannel already exists?
	newChannel := proto.Clone(pc.info).(*logpb.PChannelInfo)
	// merge channel together
	for _, vChannelName := range vChannels {
		find := false
		for _, vChannelInfo := range newChannel.VChannelInfos {
			if vChannelInfo.Name == vChannelName {
				find = true
			}
		}
		if find {
			// repeated vchannel found.
			continue
		}
		newChannel.VChannelInfos = append(newChannel.VChannelInfos, &logpb.VChannelInfo{
			Name: vChannelName,
		})
	}

	if err := pc.catalog.SavePChannel(ctx, newChannel); err != nil {
		return err
	}
	pc.info = newChannel
	return nil
}

// RemoveVChannel removes a VChannel from PChannel.
func (pc *physicalChannelImpl) RemoveVChannel(ctx context.Context, vChannels ...string) error {
	if len(vChannels) == 0 {
		return nil
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.dropped {
		return ErrDropped
	}

	// should we report error if vchannel not exists?
	newChannel := proto.Clone(pc.info).(*logpb.PChannelInfo)
	// merge channel together
	for _, vChannelName := range vChannels {
		// find the vchannel
		for i, vChannelInfo := range newChannel.VChannelInfos {
			if vChannelInfo.Name == vChannelName {
				// remove the vchannel
				newChannel.VChannelInfos = append(newChannel.VChannelInfos[:i], newChannel.VChannelInfos[i+1:]...)
				break
			}
		}
	}

	if err := pc.catalog.SavePChannel(ctx, newChannel); err != nil {
		return err
	}
	pc.info = newChannel
	return nil
}

// Drop the channel.
func (pc *physicalChannelImpl) Drop(ctx context.Context) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if len(pc.info.VChannelInfos) != 0 {
		return errors.Wrapf(
			ErrDropNotEmptyPChannel,
			fmt.Sprintf("vchannel count: %d", len(pc.info.VChannelInfos)),
		)
	}

	if err := pc.catalog.DropPChannel(ctx, pc.info.Name); err != nil {
		return err
	}
	pc.dropped = true
	return nil
}
