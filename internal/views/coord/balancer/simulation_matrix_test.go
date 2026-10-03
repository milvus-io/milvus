package balancer

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func simulationRows(kind string, shards, segments int, seed int64) [][]int64 {
	rng := rand.New(rand.NewSource(seed))
	out := make([][]int64, shards)
	for sh := range out {
		for seg := 0; seg < segments; seg++ {
			rows := int64(1_000_000)
			switch kind {
			case "tiny":
				rows = 5_000
			case "zero":
				rows = 0
			case "tail":
				rows = int64(1+rng.Intn(100)) * 10_000
				if rng.Intn(20) == 0 {
					rows *= 20
				}
			case "whale":
				rows = 10_000
				if seg == 0 {
					rows = 50_000_000
				}
			}
			out[sh] = append(out[sh], rows)
		}
	}
	return out
}

func simulationPopulate(s *balanceSimulation, count, replicas, shards, segments int, kind string, seed int64) {
	groups := make([]string, replicas)
	for i := range groups {
		groups[i] = "rg1"
	}
	for id := 1; id <= count; id++ {
		s.addCollection(int64(id), groups, simulationRows(kind, shards, segments, seed+int64(id)))
	}
}

func TestBalanceSimulationInitialDistributions(t *testing.T) {
	for _, kind := range []string{"uniform", "tiny", "tail", "whale", "zero"} {
		for _, seed := range []int64{1, 7, 42} {
			t.Run(fmt.Sprintf("%s_seed%d", kind, seed), func(t *testing.T) {
				s := newBalanceSimulation(t, 8)
				simulationPopulate(s, 4, 1, 3, 12, kind, seed)
				result := s.settle("initial", nil, 0, 40)
				if kind == "uniform" {
					require.Equal(t, "stable_in_band", result.Outcome)
				}
				stable := s.settle("unchanged", nil, 0, 10)
				require.Zero(t, stable.Counters.MovedRows)
				require.Zero(t, stable.Counters.Prepares)
			})
		}
	}
	t.Run("tiny_100_nodes", func(t *testing.T) {
		s := newBalanceSimulation(t, 100)
		simulationPopulate(s, 1, 1, 1, 8, "tiny", 1)
		r := s.settle("initial", nil, 0, 20)
		require.Equal(t, 1, r.After.FanoutMax)
	})
	t.Run("empty_shard", func(t *testing.T) {
		s := newBalanceSimulation(t, 3)
		s.addCollection(1, []string{"rg1"}, [][]int64{{}})
		s.settle("empty", nil, 0, 10)
	})
	t.Run("no_capacity_restore", func(t *testing.T) {
		s := newBalanceSimulation(t, 0)
		simulationPopulate(s, 2, 2, 1, 6, "uniform", 1)
		s.settle("no_nodes", nil, 0, 10)
		s.node(1, "rg1", true, false)
		s.settle("one_node", nil, 0, 10)
		s.node(2, "rg1", true, false)
		s.settle("two_nodes", nil, 0, 20)
	})
}

func TestBalanceSimulationTopology(t *testing.T) {
	for _, seed := range []int64{1, 7, 42} {
		for _, batch := range []int{0, 1} {
			t.Run(fmt.Sprintf("events_seed%d_batch%d", seed, batch), func(t *testing.T) {
				s := newBalanceSimulation(t, 8)
				simulationPopulate(s, 12, 1, 2, 12, "tail", seed)
				s.settle("initial", nil, batch, 40)
				for n := int64(9); n <= 12; n++ {
					s.node(n, "rg1", true, false)
				}
				s.settle("expand_8_to_12", nil, batch, 40)
				for n := int64(1); n <= 4; n++ {
					s.node(n, "rg1", true, true)
				}
				s.settle("drain_12_to_8", nil, batch, 40)
				for n := int64(1); n <= 4; n++ {
					s.node(n, "rg1", false, false)
				}
				s.settle("remove_drained", nil, batch, 20)
				s.node(5, "rg1", false, false)
				s.settle("failure", nil, batch, 40)
				s.node(13, "rg1", true, false)
				s.settle("rejoin_new_id", nil, batch, 40)
				for n := int64(6); n <= 8; n++ {
					s.node(n, "rg1", false, false)
					s.settle(fmt.Sprintf("rolling_remove_%d", n), nil, batch, 40)
					s.node(n+20, "rg1", true, false)
					s.settle(fmt.Sprintf("rolling_add_%d", n+20), nil, batch, 40)
				}
				s.policy = NewDefaultBalancePolicy()
				s.settle("policy_restart", nil, batch, 40)
			})
		}
	}
	t.Run("replacement_added_first", func(t *testing.T) {
		s := newBalanceSimulation(t, 4)
		simulationPopulate(s, 8, 1, 2, 12, "uniform", 1)
		s.settle("initial", nil, 0, 20)
		for n := int64(1); n <= 4; n++ {
			s.node(n+10, "rg1", true, false)
			s.settle("add_replacement", nil, 0, 30)
			s.node(n, "rg1", true, true)
			s.settle("drain_original", nil, 0, 30)
		}
	})
}

func TestBalanceSimulationReplicasAndRG(t *testing.T) {
	for _, nodes := range []int{3, 6, 8} {
		t.Run(fmt.Sprintf("replicas_nodes%d", nodes), func(t *testing.T) {
			s := newBalanceSimulation(t, nodes)
			simulationPopulate(s, 5, 1, 2, 12, "uniform", 1)
			s.settle("replica1", nil, 2, 30)
			for _, id := range s.collections {
				s.replicas(id, []string{"rg1", "rg1"})
			}
			s.settle("replica2", nil, 2, 40)
			for _, id := range s.collections {
				s.replicas(id, []string{"rg1", "rg1", "rg1"})
			}
			s.settle("replica3", nil, 2, 40)
			for n := 3; n <= nodes; n++ {
				s.node(int64(n), "rg1", false, false)
			}
			s.settle("shortage_two_nodes", nil, 2, 40)
			s.node(100, "rg1", true, false)
			s.settle("restore_third", nil, 2, 40)
			for _, id := range s.collections {
				s.replicas(id, []string{"rg1"})
			}
			s.settle("replica1_again", nil, 2, 40)
		})
	}
	t.Run("unequal_quota_single_collection", func(t *testing.T) {
		s := newBalanceSimulation(t, 3)
		simulationPopulate(s, 1, 2, 1, 12, "uniform", 1)
		r := s.settle("two_replicas_three_nodes", nil, 0, 30)
		require.Equal(t, "stable_outside_band", r.Outcome)
	})
	t.Run("rg_node_and_replica_migration", func(t *testing.T) {
		s := newBalanceSimulation(t, 4)
		for n := int64(5); n <= 8; n++ {
			s.node(n, "rg2", true, false)
		}
		for id := int64(1); id <= 8; id++ {
			s.addCollection(id, []string{"rg1", "rg2"}, simulationRows("tail", 2, 12, id))
		}
		s.settle("two_groups", nil, 2, 40)
		s.node(4, "rg2", true, false)
		s.settle("node_rg1_to_rg2", nil, 2, 40)
		s.node(5, "rg1", true, false)
		s.settle("node_rg2_to_rg1", nil, 2, 40)
		for _, id := range s.collections[:4] {
			s.replicas(id, []string{"rg2", "rg2"})
		}
		s.settle("replica_rg_change", nil, 2, 40)
	})
}

func TestBalanceSimulationDataAndLoadChanges(t *testing.T) {
	s := newBalanceSimulation(t, 6)
	simulationPopulate(s, 12, 1, 3, 8, "tail", 7)
	s.settle("initial", nil, 3, 40)
	for _, id := range s.collections[:6] {
		s.replicas(id, nil)
	}
	s.settle("release_half", nil, 3, 40)
	for _, id := range s.collections[:6] {
		s.replicas(id, []string{"rg1"})
	}
	s.settle("reload_half", nil, 3, 40)
	for _, id := range s.collections {
		s.data(id, simulationRows("tail", 3, 12, id), false)
	}
	s.settle("append_segments", nil, 3, 40)
	for _, id := range s.collections {
		s.data(id, simulationRows("uniform", 3, 4, id), true)
	}
	s.settle("compaction_new_ids", nil, 3, 40)
	for _, id := range s.collections {
		s.data(id, simulationRows("tiny", 3, 4, id), true)
	}
	s.settle("shrink_rows", nil, 3, 40)
	for _, id := range s.collections {
		s.data(id, [][]int64{{}, {}, {}}, true)
	}
	s.settle("empty_all_shards", nil, 3, 40)
	for _, id := range s.collections {
		s.replicas(id, nil)
	}
	r := s.settle("release_all", nil, 3, 20)
	require.Empty(t, s.views)
	require.Zero(t, r.After.ActiveShards)
}

func TestBalanceSimulationPreparingAndApply(t *testing.T) {
	t.Run("delay_partial_failure", func(t *testing.T) {
		s := newBalanceSimulation(t, 3)
		simulationPopulate(s, 1, 1, 1, 6, "uniform", 1)
		before := s.counters
		s.step(s.collections, -1, false)
		ids := s.scope(s.collections)
		id := ids[0]
		v := s.views[id]
		for seg := range v.preparing.assignments {
			if seg%3 != 0 {
				v.preparing.ready[seg] = true
			}
		}
		s.publish(id)
		for i := 0; i < 5; i++ {
			p := s.step(s.collections, -1, false)
			require.Empty(t, p.Prepares)
		}
		held := s.sample(false)
		require.Equal(t, 1, held.Pending)
		require.Zero(t, held.UnplacedRows)
		v.failed, v.preparing = v.preparing, nil
		s.publish(id)
		r := s.settle("partial_failure_retry", nil, 0, 30)
		require.Less(t, r.Counters.LoadRows, int64(6_000_000))
		s.emit(simulationResult{Scenario: t.Name(), Phase: "including_failed_attempt", Outcome: "stable_in_band", Counters: simulationCounterDelta(s.counters, before), After: s.sample(true), Config: *s.cache.GetBalanceConfig()})
	})
	t.Run("node_loss_while_preparing", func(t *testing.T) {
		s := newBalanceSimulation(t, 3)
		simulationPopulate(s, 4, 1, 2, 6, "uniform", 1)
		s.step(s.collections, -1, false)
		s.node(1, "rg1", false, false)
		s.settle("repair_lost_preparing", nil, 0, 40)
	})
	for _, cap := range []int{0, 2} {
		t.Run(fmt.Sprintf("apply_cap_%d", cap), func(t *testing.T) {
			s := newBalanceSimulation(t, 4)
			simulationPopulate(s, 10, 1, 1, 8, "uniform", 1)
			s.step(s.collections, cap, true)
			r := s.settle("retry_after_partial_apply", nil, 0, 30)
			require.Equal(t, "stable_in_band", r.Outcome)
		})
	}
}

func TestBalanceSimulationConfigAndBudget(t *testing.T) {
	for _, price := range []float64{0, 0.1, 1} {
		t.Run(fmt.Sprintf("price_%g", price), func(t *testing.T) {
			s := newBalanceSimulation(t, 2)
			simulationPopulate(s, 4, 1, 2, 12, "uniform", 1)
			s.settle("initial", nil, 0, 20)
			cfg := DefaultBalanceConfig()
			cfg.MovePrice = price * .2
			cfg.LoadPrice = price * .8
			s.cache.UpdateBalanceConfig(cfg)
			s.node(3, "rg1", true, false)
			s.settle("expand_with_price", nil, 0, 40)
		})
	}
	for _, budget := range []int64{1, 8, 100000} {
		t.Run(fmt.Sprintf("budget_%d", budget), func(t *testing.T) {
			s := newBalanceSimulation(t, 2)
			simulationPopulate(s, 1, 1, 1, 6, "uniform", 1)
			s.settle("initial", nil, 0, 20)
			cfg := DefaultBalanceConfig()
			cfg.MaxCandidateEvaluations = budget
			s.cache.UpdateBalanceConfig(cfg)
			s.node(3, "rg1", true, false)
			r := s.settle("expand", nil, 0, 300)
			require.Equal(t, "stable_in_band", r.Outcome)
		})
	}
	t.Run("auto_balance", func(t *testing.T) {
		s := newBalanceSimulation(t, 2)
		simulationPopulate(s, 4, 1, 2, 12, "uniform", 1)
		s.settle("initial", nil, 0, 20)
		cfg := DefaultBalanceConfig()
		cfg.AutoBalance = false
		s.cache.UpdateBalanceConfig(cfg)
		s.node(3, "rg1", true, false)
		r := s.settle("disabled_expand", nil, 0, 20)
		require.Zero(t, r.Counters.Prepares)
		cfg.AutoBalance = true
		s.cache.UpdateBalanceConfig(cfg)
		s.settle("enabled", nil, 0, 30)
	})
}

func TestBalanceSimulationPartialScopes(t *testing.T) {
	for _, count := range []int{100, 10000} {
		for _, batch := range []int{0, 1, 25, 100} {
			if count == 10000 && batch == 1 {
				continue
			}
			t.Run(fmt.Sprintf("collections%d_batch%d", count, batch), func(t *testing.T) {
				s := newBalanceSimulation(t, 4)
				simulationPopulate(s, count, 1, 1, 4, "tiny", 42)
				s.settle("initial", nil, batch, 20)
				s.node(5, "rg1", true, false)
				s.node(6, "rg1", true, false)
				s.settle("expand_rotating_scope", nil, batch, 30)
			})
		}
	}
	t.Run("permanently_restricted_scope", func(t *testing.T) {
		s := newBalanceSimulation(t, 2)
		simulationPopulate(s, 100, 1, 1, 6, "uniform", 1)
		s.settle("initial", nil, 25, 20)
		s.node(3, "rg1", true, false)
		r := s.settle("only_one_collection", s.collections[:1], 1, 30)
		require.Equal(t, "stable_outside_band", r.Outcome)
		s.settle("restore_full_coverage", nil, 25, 40)
	})
}

// Model consistency is checked even before quiescence: accepted Preparing
// replaces Up rather than adding another complete target copy.
func TestBalanceSimulationTargetReplacement(t *testing.T) {
	s := newBalanceSimulation(t, 2)
	simulationPopulate(s, 1, 1, 1, 6, "uniform", 1)
	s.settle("initial", nil, 0, 20)
	s.node(3, "rg1", true, false)
	s.step(s.collections, -1, false)
	var sum int64
	s.cache.RangeNodeIDs(func(id int64) bool { sum += s.cache.GetNode(id).TargetRows(); return true })
	require.Equal(t, int64(6_000_000), sum)
	for id, v := range s.views {
		if v.preparing != nil {
			s.complete(id)
		}
	}
	s.settle("replacement_complete", nil, 0, 20)
}

func TestBalanceSimulationCanonicalRecovery(t *testing.T) {
	s := newBalanceSimulation(t, 3)
	simulationPopulate(s, 1, 1, 1, 6, "uniform", 1)
	s.settle("initial", nil, 0, 20)
	s.node(1, "rg1", false, false)
	s.settle("node1_lost", nil, 0, 20)
	s.node(4, "rg1", true, false)
	r := s.settle("node4_joined", nil, 0, 20)
	for _, rows := range r.After.Groups["rg1"].Rows {
		require.Equal(t, int64(2_000_000), rows)
	}
	require.Equal(t, int64(2_000_000), r.Counters.MovedRows)
}

func TestBalanceSimulationFanoutGrowthShrink(t *testing.T) {
	s := newBalanceSimulation(t, 4)
	for _, total := range []int64{80_000, 105_000, 120_000, 105_000, 95_000, 80_000} {
		rows := [][]int64{{total / 4, total / 4, total / 4, total / 4}}
		if len(s.collections) == 0 {
			s.addCollection(1, []string{"rg1"}, rows)
		} else {
			s.data(1, rows, false)
		}
		s.settle(fmt.Sprintf("rows_%d", total), nil, 0, 30)
	}
}

// Diagnostic input outside the normal fixed-vchannel lifecycle. Record residual
// work rather than treating a search cap or a retained old shard as convergence.
func TestBalanceSimulationRetiredShardDiagnostic(t *testing.T) {
	s := newBalanceSimulation(t, 3)
	simulationPopulate(s, 1, 1, 2, 6, "uniform", 1)
	s.settle("initial", nil, 0, 20)
	s.data(1, simulationRows("uniform", 1, 6, 1), false)
	start := s.counters
	before := s.sample(false)
	for i := 0; i < 10; i++ {
		s.step(s.collections, -1, true)
	}
	var rows int64
	s.cache.RangeNodeIDs(func(id int64) bool { rows += s.cache.GetNode(id).TargetRows(); return true })
	desired := s.cache.GetCollection(1).DataView().TotalRows
	outcome := "stable_in_band"
	if rows != desired || s.counters.Retries > start.Retries {
		outcome = "retired_shard_residual"
	}
	s.emit(simulationResult{Scenario: t.Name(), Phase: "remove_vchannel", Outcome: outcome, Sweeps: 10, Scope: 1, Batch: 1, Before: before, After: s.sample(false), Counters: simulationCounterDelta(s.counters, start), Config: *s.cache.GetBalanceConfig()})
	t.Logf("SIM_DIAGNOSTIC retired-shard target_rows=%d desired_rows=%d retries=%d", rows, desired, s.counters.Retries-start.Retries)
}
