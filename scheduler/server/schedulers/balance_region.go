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
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
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

	//1.挑选出所有适合的store
	//依据：是up的以及down的时间小于StoreDownTime
	suitableStores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		//Q:Up和Down的含义？
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	//如果store的数量不够，返回
	if len(suitableStores) <= 1 {
		return nil
	}
	//对suitavleStores按RegionSize进行排序
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})

	//2.选择需要移动的region
	//依据：size最大，优先度：pending>follower>leader
	var regionInfo *core.RegionInfo
	var sourceStore, dstStore *core.StoreInfo
	for i, store := range suitableStores {
		sourceStore = suitableStores[i]
		//2.1 先考察PendingRegion，因为最可能磁盘过载
		cluster.GetPendingRegionsWithLock(store.GetID(), func(regions core.RegionsContainer) {
			regionInfo = regions.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
		//2.2 考察Follower，移动代价小
		cluster.GetFollowersWithLock(store.GetID(), func(regions core.RegionsContainer) {
			regionInfo = regions.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
		//2.3 最后考察Leader，移动代价大
		cluster.GetLeadersWithLock(store.GetID(), func(regions core.RegionsContainer) {
			regionInfo = regions.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
	}
	//如果没有找到移动的region，放弃操作
	if regionInfo == nil {
		return nil
	}
	//3.如果region的store数量小于cluster.GetMaxReplicas，放弃操作
	if len(regionInfo.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}
	//4.找适合移动至的目标store
	//依据：size最小，不能是原来的region所在store
	for i := range suitableStores {
		store := suitableStores[len(suitableStores)-i-1]
		if store.GetID() != sourceStore.GetID() {
			dstStore = store
			break
		}
	}
	if dstStore == nil {
		return nil
	}
	//5.验证这次移动是否有价值
	//依据：目标store和原store的size之差是否大于2倍region的approximateSize
	//上个步骤中直接取size最小的store做验证就可以了，因为最小的若不能满足，次小的一定不能满足
	if sourceStore.GetRegionSize()-dstStore.GetRegionSize() < 2*regionInfo.GetApproximateSize() {
		return nil
	}

	//6.调用

	return nil
}
