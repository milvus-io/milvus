package componentutil

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// NewComponentStateService create a ComponentStateService
func NewComponentStateService(role string) *ComponentStateService {
	return &ComponentStateService{
		nodeID:    common.NotRegisteredID,
		role:      role,
		stateCode: commonpb.StateCode_StandBy,
	}
}

// ComponentStateService is a helper type to implement a GetComponentStates rpc at server side.
// StandBy -> Initializing -> Healthy -> Abnormal -> Healthy
// All can transfer into Stopping
type ComponentStateService struct {
	mu        sync.Mutex
	nodeID    int64
	role      string
	stateCode commonpb.StateCode
}

// OnInitializing set the state to initializing
func (s *ComponentStateService) OnInitializing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stateCode != commonpb.StateCode_StandBy {
		panic("standby -> initializing")
	}
	s.stateCode = commonpb.StateCode_Initializing
}

func (s *ComponentStateService) OnInitialized(nodeID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeID = nodeID
	if s.stateCode != commonpb.StateCode_Initializing {
		panic("initializing -> healthy")
	}
	s.stateCode = commonpb.StateCode_Healthy
}

func (s *ComponentStateService) OnHealthy() {
	s.mu.Lock()
	if s.stateCode == commonpb.StateCode_Abnormal {
		s.stateCode = commonpb.StateCode_Healthy
	}
	s.mu.Unlock()
}

func (s *ComponentStateService) OnAbnormal() {
	s.mu.Lock()
	if s.stateCode == commonpb.StateCode_Healthy {
		s.stateCode = commonpb.StateCode_Abnormal
	}
	s.mu.Unlock()
}

func (s *ComponentStateService) OnStopping() {
	s.mu.Lock()
	s.stateCode = commonpb.StateCode_Stopping
	s.mu.Unlock()
}

// GetComponentStates get the component state of a milvus node.
func (s *ComponentStateService) GetComponentStates(ctx context.Context, _ *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	s.mu.Lock()
	code := s.stateCode
	nodeID := s.nodeID
	s.mu.Unlock()

	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    nodeID,
			Role:      s.role,
			StateCode: code,
		},
		Status: merr.Status(nil),
	}, nil
}
