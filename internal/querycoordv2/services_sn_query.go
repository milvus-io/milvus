package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetQueryViewLoadInfo exposes the collection load configuration shared by SN and QN.
func (s *Server) GetQueryViewLoadInfo(ctx context.Context, req *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
	resp := &querypb.GetQueryViewLoadInfoResponse{
		Status:       merr.Success(),
		CollectionID: req.GetCollectionID(),
	}
	if err := merr.CheckHealthy(s.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if req.GetCollectionID() == 0 {
		resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg("collection id is zero"))
		return resp, nil
	}
	if s.qviewsRuntime == nil || s.qviewsRuntime.loadConfigStore == nil {
		resp.Status = merr.Status(merr.WrapErrServiceInternalMsg("query view load metadata is not initialized"))
		return resp, nil
	}
	if req.GetSplitAdmissionOnly() {
		allowed, err := s.qviewsRuntime.splitTargetAdmission(ctx, req)
		resp.SplitAdmissionAllowed = allowed
		if err != nil {
			resp.Status = merr.Status(err)
		}
		return resp, nil
	}
	info, err := s.qviewsRuntime.loadConfigStore.GetLoadInfo(ctx, req.GetCollectionID(), req.GetVersion())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if req.GetVersion() == 0 && s.qviewsRuntime.balanceCache != nil {
		entry := s.qviewsRuntime.balanceCache.GetCollection(req.GetCollectionID())
		if entry != nil && entry.DataView() != nil && entry.DataView().ShardSetEpoch > 0 {
			version := entry.DataView().DataVersion.IntoProto()
			ref, err := s.qviewsRuntime.resources.DataViews.Get(ctx, req.GetCollectionID(), version)
			if err != nil {
				resp.Status = merr.Status(err)
				return resp, nil
			}
			if ref == nil {
				resp.Status = merr.Status(merr.WrapErrServiceNotReadyMsg("serving DataView unavailable"))
				return resp, nil
			}
			defer ref.Deref()
			cfg := s.qviewsRuntime.loadConfigStore.GetConfig(req.GetCollectionID())
			if cfg == nil {
				resp.Status = merr.Status(merr.WrapErrCollectionNotLoaded(req.GetCollectionID()))
				return resp, nil
			}
			generation, err := s.qviewsRuntime.resolveServingGeneration(ctx, ref.DataView(), cfg)
			if err != nil {
				resp.Status = merr.Status(err)
				return resp, nil
			}
			// Resolve all load scope from this publication, never from the earlier
			// latest lookup that may have raced a same-epoch Release/Reload.
			info, err = s.qviewsRuntime.loadConfigStore.GetLoadInfo(ctx, req.GetCollectionID(), generation.GetLoadInfoVersion())
			if err != nil {
				resp.Status = merr.Status(err)
				return resp, nil
			}
			info.ShardSetEpoch, info.ServingShards = generation.GetShardSetEpoch(), generation.GetShards()
			info.ReadSchemaVersion = generation.GetReadSchemaVersion()
		}
	}
	return info, nil
}

// Serialize publication with load/release so a captured config cannot republish
// an obsolete load identity after StopPlanning or a concurrent Reload.
func (r *qviewsRuntime) resolveServingGeneration(ctx context.Context, desired *viewpb.DataViewOfCollection, cfg *loadmgr.LoadConfig) (*viewpb.ServingGeneration, error) {
	var generation *viewpb.ServingGeneration
	err := r.loadConfigStore.WithCurrentConfig(cfg, func() error {
		replicas := make([]int64, 0, len(cfg.Replicas))
		for _, replica := range cfg.Replicas {
			replicas = append(replicas, replica.ReplicaID)
		}
		var err error
		generation, err = r.shardViewRegistry.ResolveServingGeneration(ctx, desired, cfg.Version, replicas)
		return err
	})
	return generation, err
}
