// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	storeInfos := cluster.GetStores()
	suitableStores := make([]*core.StoreInfo, 0)
	maxStoreDownTime := cluster.GetMaxStoreDownTime()
	for _, storeInfo := range storeInfos {
		if storeInfo.DownTime() < maxStoreDownTime {
			suitableStores = append(suitableStores, storeInfo)
		}
	}
	if len(suitableStores) <= 1 {
		return nil
	}
	sort.Slice(suitableStores, func(i, j int) bool {
		if suitableStores[i].GetRegionSize() != suitableStores[j].GetRegionSize() {
			return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
		}
		return suitableStores[i].DownTime() > suitableStores[j].DownTime()
	})
	var selectedRegion *core.RegionInfo
	var selectedStore *core.StoreInfo
	for _, suitableStore := range suitableStores {
		cluster.GetPendingRegionsWithLock(suitableStore.GetID(), func(container core.RegionsContainer) {
			selectedRegion = container.RandomRegion(nil, nil)
		})
		if selectedRegion != nil {
			selectedStore = suitableStore
			break
		}
		cluster.GetFollowersWithLock(suitableStore.GetID(), func(container core.RegionsContainer) {
			selectedRegion = container.RandomRegion(nil, nil)
		})
		if selectedRegion != nil {
			selectedStore = suitableStore
			break
		}
		cluster.GetLeadersWithLock(suitableStore.GetID(), func(container core.RegionsContainer) {
			selectedRegion = container.RandomRegion(nil, nil)
		})
		if selectedRegion != nil {
			selectedStore = suitableStore
			break
		}
	}
	if selectedRegion == nil {
		return nil
	}
	if len(selectedRegion.GetPeers()) != cluster.GetMaxReplicas() {
		return nil
	}
	var targetStore *core.StoreInfo
	for i := len(suitableStores) - 1; i >= 0; i-- {
		selectedRegionStores := cluster.GetRegionStores(selectedRegion)
		isIN := false
		for _, selectedRegionStore := range selectedRegionStores {
			if selectedRegionStore == suitableStores[i] {
				isIN = true
				break
			}
		}
		if !isIN {
			targetStore = suitableStores[i]
			break
		}
	}
	if targetStore == nil {
		return nil
	}
	if selectedStore.GetRegionSize()-targetStore.GetRegionSize() <= 2*selectedRegion.GetApproximateSize() {
		return nil
	}
	peer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}
	peerOperator, err := operator.CreateMovePeerOperator("move-peer", cluster, selectedRegion, operator.OpBalance, selectedStore.GetID(), targetStore.GetID(), peer.GetId())
	if err != nil {
		return nil
	}
	return peerOperator
}
