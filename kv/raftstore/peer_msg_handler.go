package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

//extra func by Sunlly
func isPeerExist(region *metapb.Region, id uint64) bool {
	for _, p := range region.Peers {
		if p.Id == id {
			return true
		}
	}
	return false
}

//extra key from req by Sunlly
func (d *peerMsgHandler) getRequestKey(req *raft_cmdpb.Request) []byte {
	var key []byte
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	case raft_cmdpb.CmdType_Snap:
	}
	return key
}

//by Sunlly
func (d *peerMsgHandler) handleProposal(entry *eraftpb.Entry, handle func(*proposal)) {
	// if len(d.proposals) > 0 {
	// 	p := d.proposals[0]
	// 	if p.index == entry.Index {
	// 		if p.term != entry.Term {
	// 			NotifyStaleReq(entry.Term, p.cb)
	// 		} else {
	// 			handle(p)
	// 		}
	// 	}
	// 	if p.index > entry.Index {
	// 		return
	// 	}
	// 	if entry.Term == p.term && entry.Index > p.index {
	// 		NotifyStaleReq(p.term, p.cb)
	// 	}
	// 	d.proposals = d.proposals[1:]
	// }
	for len(d.proposals) > 0 {
		proposal := d.proposals[0]
		//1.过期的entry，已经被其他执行了
		if entry.Term < proposal.term {
			return
		}
		//2.更高任期的entry, 本条proposal作废
		if entry.Term > proposal.term {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}
		//3.entry的index小，不回复
		if entry.Term == proposal.term && entry.Index < proposal.index {
			return
		}
		//4.entry的index大，回复发送至错误的领导人
		if entry.Term == proposal.term && entry.Index > proposal.index {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}
		//5.entry的index等于proposal.index
		if entry.Index == proposal.index && entry.Term == proposal.term {
			handle(proposal)
			d.proposals = d.proposals[1:]
			// if response.Header == nil {
			// 	response.Header = &raft_cmdpb.RaftResponseHeader{}
			// }
			// // proposal.cb.Txn = txn
			// proposal.cb.Done(response)
		}
	}
}

//用于handleRaftReady中应用entry中的命令请求 by Sunlly
func (d *peerMsgHandler) process(entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := &eraftpb.ConfChange{}
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		d.processConfChange(entry, cc, wb)
		return
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	if err := msg.Unmarshal(entry.Data); err != nil {
		panic(err)
	}
	if len(msg.Requests) > 0 {
		d.processRequest(entry, msg, wb)
	} else if msg.AdminRequest != nil {
		d.processAdminRequest(entry, msg, wb)
	}
}

//3b by Sunlly0
func (d *peerMsgHandler) processConfChange(entry *eraftpb.Entry, cc *eraftpb.ConfChange, wb *engine_util.WriteBatch) {
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}

	//1.使用util.CheckRegionEpoch判断配置是否落后，否则继续
	region := d.Region()
	err = util.CheckRegionEpoch(msg, region, true)
	if err != nil {
		panic(err)
		return
	}
	log.Infof("%d processConfChange", d.PeerId())
	//2.依据changetype(addnode,removenode)做不同的执行
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		//3.对于addnode，为region增加peer，并持久化region
		//Q:为啥要做这些操作？
		//为避免重复添加，应该在添加前判断该peer是否已经在region中
		if !isPeerExist(region, cc.NodeId) {
			//将peer添加到region
			peer := msg.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers, peer)
			//ConfVer版本号++
			region.RegionEpoch.ConfVer++
			//持久化region信息（kvdb）
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			//更新storemeta中的region
			// storeMeta := d.ctx.storeMeta
			// storeMeta.Lock()
			// storeMeta.regions[region.Id] = region
			// storeMeta.Unlock()
			//为Cache增加该peer
			d.insertPeerCache(peer)
			// d.RaftGroup.ApplyConfChange(*cc)
		}

	case eraftpb.ConfChangeType_RemoveNode:
		//4.对于removenode
		//4.1 如果待删除的节点是自己，则调用destroyPeer，其他不用管
		if cc.NodeId == d.Meta.Id {
			log.Infof("processConfChange: %d remove %d self", d.PeerId(), cc.NodeId)
			d.destroyPeer()
			return
		}
		//4.2 删除的是其他节点
		if isPeerExist(region, cc.NodeId) {
			log.Infof("processConfChange: %d remove %d", d.PeerId(), cc.NodeId)
			for i, p := range region.Peers {
				if p.Id == cc.NodeId {
					region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
					break
				}
			}
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			// storeMeta := d.ctx.storeMeta
			// storeMeta.Lock()
			// storeMeta.regions[region.Id] = region
			// storeMeta.Unlock()
			d.removePeerCache(cc.NodeId)
		}

	}
	//5.在下层的raft中应用该ConfChange，调用addnode和removenode函数
	d.RaftGroup.ApplyConfChange(*cc)
	//6.回复callback
	//Q:是否需要对重复的process做callback?
	d.handleProposal(entry, func(p *proposal) {
		p.cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{},
			},
		})
	})
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}

}

//2c 快照 by Sunlly
func (d *peerMsgHandler) processAdminRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) {
	req := msg.AdminRequest
	// resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	switch req.CmdType {
	//1.快照命令的处理
	case raft_cmdpb.AdminCmdType_CompactLog:
		log.Infof("*** %d process CompactLog:", d.regionId)
		compactLog := req.GetCompactLog()
		//如果compactLog命令的值更大，更新截断的Index和Term
		if compactLog.CompactIndex >= d.peerStorage.truncatedIndex() {
			d.peerStorage.applyState.TruncatedState.Index = compactLog.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			d.ScheduleCompactLog(d.peerStorage.truncatedIndex())
		}
	case raft_cmdpb.AdminCmdType_Split:
		//0.检查region
		region := d.Region()
		err := util.CheckRegionEpoch(msg, region, true)
		if err != nil {
			// d.handleProposal(entry,func(p *proposal) {
			// 	p.cb.Done(err)
			// })
			panic(err)
		}
		split := req.GetSplit()
		err2 := util.CheckKeyInRegion(split.SplitKey, region)
		if err2 != nil {
			panic(err2)
		}
		//1.生成新region
		util.CloneMsg()
	}

}

//by Sunlly
func (d *peerMsgHandler) processRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.Requests[0]
	key := d.getRequestKey(req)
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return wb
		}
	}
	// apply to kv (Put/Delete)
	// 每个客户端都需要处理
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
	case raft_cmdpb.CmdType_Put:
		wb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	case raft_cmdpb.CmdType_Delete:
		wb.DeleteCF(req.Delete.Cf, req.Delete.Key)
	case raft_cmdpb.CmdType_Snap:
	}
	//callback after applying (Get/Snap)
	//只用一个客户端（接收到proposal的）处理并返回就可以了
	//回复消息
	d.handleProposal(entry, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			if err != nil {
				value = nil
			}
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get,
				Get: &raft_cmdpb.GetResponse{Value: value}}}
			//Q:为啥此处要new WB?
			wb = new(engine_util.WriteBatch)
		case raft_cmdpb.CmdType_Put:
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put,
				Put: &raft_cmdpb.PutResponse{}}}
		case raft_cmdpb.CmdType_Delete:
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete,
				Delete: &raft_cmdpb.DeleteResponse{}}}
		case raft_cmdpb.CmdType_Snap:
			//Q:为何这样处理Snap？
			if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
				return
			}
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
		// log.Infof("***---processRequestResp: %d, index: %d, resp:%s", d.RaftGroup.GetRaftId(), entry.Index, resp)
		p.cb.Done(resp)
	})
	// noop entry
	return wb
}

//2B
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	//1.判断是否有新的Ready，如果没有则不用处理,如果有则生成Ready
	if !d.RaftGroup.HasReady() {
		return
	}
	log.Infof("--- handleRaftReady %d ", d.RaftGroup.GetRaftId())
	rd := d.RaftGroup.Ready()
	//2.将Ready持久化到badger.(保存Raftdb的信息)，必须持久化之后才处理Ready
	result, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}
	if result != nil {
		// PrevRegion
		//Q:为何这样处理？
		// if !reflect.DeepEqual(result.PrevRegion, result.Region) {
		// 	d.peerStorage.SetRegion(result.Region)
		// 	storeMeta := d.ctx.storeMeta
		// 	storeMeta.regions[result.Region.Id] = result.Region
		// 	storeMeta.regionRanges.Delete(&regionItem{region: result.PrevRegion})
		// 	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
		// }
	}
	//3.调用d.send方法将Ready中的Messagge发送出去
	if len(rd.Messages) != 0 {
		for _, msg := range rd.Messages {
			if msg.Snapshot != nil {
				log.Infof("%d handleRaftReady:msg with snapshot", d.RaftGroup.GetRaftId())
			}
		}
		d.Send(d.ctx.trans, rd.Messages)
	}
	//4.应用ready.CommittedEntries中的entry（kvdb）
	if len(rd.CommittedEntries) > 0 {
		// log.Infof("***---process committedEntries: %d, len:%d", d.RaftGroup.GetRaftId(), len(rd.CommittedEntries))
		//遍历ready.CommittedEntries，逐条应用于kvdb
		// oldProposals := d.proposals
		for _, entry := range rd.CommittedEntries {
			// log.Infof("***----process committed: %d, index:%d", d.RaftGroup.GetRaftId(), entry.Index)
			//4.1为写入磁盘，定义WriteBatch
			kvWB := new(engine_util.WriteBatch)
			//4.2 执行命令，4.3回复消息
			d.process(&entry, kvWB)
			//4.4 更新applied Index
			d.peerStorage.applyState.AppliedIndex = entry.Index
			//4.5 存储Raft状态（apply）
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if d.stopped {
				return
			}
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
		}
		//Q:这一段代码的目的是啥？
		// if len(oldProposals) > len(d.proposals) {
		// 	proposals := make([]*proposal, len(d.proposals))
		// 	copy(proposals, d.proposals)
		// 	d.proposals = proposals
		// }
		// d.peerStorage.applyState.AppliedIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	//5.调用Advance通知RawNode，Ready处理完毕
	d.RaftGroup.Advance(rd)

}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// 对Admin和一般request分别做处理
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeRequest(msg, cb)
	}
}

//a switch of proposeRaftCommand by Sunlly
func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		log.Infof("*** %d propose CompactLog:", d.regionId)
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		//只需要leader一个节点执行，所以不用提议
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
		resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		}
		cb.Done(resp)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		//每个节点都执行，用confchange消息的方式提议
		log.Infof("%d propose ChangePeer:", d.PeerId())
		// if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
		// 	return
		// }
		context, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		//包装消息,附带chengeType(add/remove)和节点信息
		cc := eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    context,
		}
		proposal := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, proposal)
		d.RaftGroup.ProposeConfChange(cc)
	case raft_cmdpb.AdminCmdType_Split:
		log.Infof("%d propose Split:", d.PeerId())
		//1.检查命令中的key是否在region中
		err := util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
		}
		//2.转换cmd消息为日志数据，无误则继续
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		//3.保存proposal
		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		d.RaftGroup.Propose(data)
	}
}

//a switch of proposeRaftCommand by Sunlly
func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	//1.提取Request中的key,判断是否在region中，无误则进行下一步操作，否则返回错误
	req := msg.Requests[0]
	key := d.getRequestKey(req)
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	//2.转换cmd消息为日志数据，无误则继续
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	//3.保存proposal
	p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
	d.proposals = append(d.proposals, p)
	//4.调用Propose函数处理消息（MsgType_MsgPropose）
	// log.Infof("**--- proposeRequest %d, req:%s ", d.RaftGroup.GetRaftId(), req.String())
	d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
