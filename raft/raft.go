// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	//extra structure by Sunlly0
	//Peers []uint64
	VotedFor              uint64
	VotedReject           uint64
	randomElectionTimeout int
	//

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	//return Raft
	r := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	// log.Infof("NewRaft: %d", r.id)

	//提取Storage中的硬状态,否则测试不能通过
	hardState, confStat, _ := c.Storage.InitialState()
	//恢复Commit
	r.RaftLog.committed = hardState.Commit
	//恢复ApplyState
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}

	if c.peers == nil {
		c.peers = confStat.Nodes
	}

	lastLogIndex := r.RaftLog.LastIndex()
	log.Infof("++----newRaftLog: %d, len:%d, lastLogIndex:%d, committed:%d, applied:%d", r.id, len(r.RaftLog.entries), lastLogIndex, r.RaftLog.committed, r.RaftLog.applied)
	//Q:如何存储peer
	//A：刚开始想的是在Raft中增加一个数据结构，后来参考网上，可以直接使用Prs存储
	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{Next: lastLogIndex + 1, Match: lastLogIndex}
		} else {
			r.Prs[peer] = &Progress{Next: lastLogIndex + 1}
		}
	}
	r.becomeFollower(0, None)

	//恢复节点HardState
	r.Term, r.Vote = hardState.Term, hardState.Vote

	return r
}

//softState and hardState by Sunlly
func (r *Raft) getSoftState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) getHardState() pb.HardState {
	hardState := pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
	return hardState
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//Q:为啥需要添加这个条件？是因为Next==0的时候一定是空的entries吗？
	if r.Prs[to].Match == 0 && r.Prs[to].Next == 0 {
		return false
	}
	entries := make([]*pb.Entry, 0)
	nextIndex := r.Prs[to].Next
	//如果不对next==0做判断，此处的prevLogIndex将为负数
	prevLogIndex := nextIndex - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	// lastLogIndex, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	lastLogIndex := r.RaftLog.LastIndex()

	//entries：从跟随者的nextIndex->leader的lastIndex
	// if prevLogIndex >= 0 && prevLogIndex < lastLogIndex {
	// 	entries = r.RaftLog.Slice(nextIndex, lastLogIndex)
	// }
	if prevLogIndex < lastLogIndex {
		entries = r.RaftLog.Slice(nextIndex, lastLogIndex)
	}
	// log.Infof("%d,%d", r.Prs[to].Match, r.Prs[to].Next)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	// log.Infof("--+++ %d send Append to %d", r.id, to)
	r.msgs = append(r.msgs, msg)
	//DEBUG by Sunlly

	///
	return true
}

func (r *Raft) sendAppendResponse(to uint64, success bool, logTerm uint64, logIndex uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  !success,
		LogTerm: logTerm,
		Index:   logIndex,
	}
	r.msgs = append(r.msgs, msg)
	// log.Infof("--+ %d sendAppendResponse to %d, reject: %t", r.id, to, !success)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// send by Leader
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		//Q:为啥Heartbeat会考虑commit?
		Commit: commit,
	}
	r.msgs = append(r.msgs, msg)
}

//send by Follower, in normal situation
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	// by Sunlly0
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		//Q:为啥HeartbearResponse会考虑reject?
		//A：每个消息都带有任期信息，如果接收者发现m.Term>r.Term，
		//直接变为跟随者，并在response消息中附上Reject=true
		Reject: reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendRequestVote by Candidate
func (r *Raft) sendRequestVote(to uint64, lastLogTerm uint64, lastLogIndex uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastLogIndex,
	}
	r.msgs = append(r.msgs, msg)
	// log.Infof("---++ %d sendRequestVote: to %d", r.id, to)
}

// sendRequestVoteResponse by Follower, (or Candidate & Leader)
func (r *Raft) sendRequestVoteResponse(to uint64, voteGranted bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  !voteGranted,
	}
	r.msgs = append(r.msgs, msg)
}

//Reset Timeout
func (r *Raft) resetTimeout() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	//增加随机选举时间
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	//1.时间增加
	// log.Infof("--- tick: %d, electionElapsed:%d", r.id, r.electionElapsed)
	//2.判断是否超时
	switch r.State {
	//2.1 Follower、candidate选举超时，处理：变成候选者，重新选举
	case StateFollower, StateCandidate:
		r.electionElapsed++
		//利用随机选举时间进行判断
		if r.electionElapsed >= r.randomElectionTimeout {
			r.resetTimeout()
			// log.Infof("---+ tick: %d Timeout ", r.id)
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
		//2.2 Leader心跳超时，处理：更新心跳并bcast心跳给所有追随者
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.resetTimeout()
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	//1.状态更新为Follower
	r.resetTimeout()
	r.State = StateFollower
	//2.设置任期和领导者
	if term > r.Term {
		r.Term = term
	}
	r.Lead = lead
	// log.Infof("+ becomFollower: %d", r.id)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	//1.状态更新为Candidate，
	r.State = StateCandidate
	//任期++，
	r.Term++
	//清除以前的选票
	r.VotedReject = 0
	r.VotedFor = 0
	r.votes = make(map[uint64]bool, 0)
	//自己给自己投票
	r.Vote = r.id
	r.votes[r.id] = true
	r.VotedFor++

	// log.Infof("++ %d becomeCadidate", r.id)
	//根据测试逻辑，becomeCandidate中只做状态修改，不发送选举消息
}

func (r *Raft) bcastRequestVote() {
	//发送选举信息
	//候选者发送选举消息给除自己以外所有的peers
	// log.Infof("--++ %d bcastRequestVote", r.id)
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer, lastLogTerm, lastLogIndex)
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// log.Infof("+++ %d becomeLeader", r.id)
	//1. 状态转换
	if r.State != StateLeader {
		r.resetTimeout()
		r.State = StateLeader
		r.Lead = r.id
	}
	//2.对于leader，会维护每个节点的日志状态，初始化Next为lastLogIndex+1
	lastLogIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		if peer != r.id {
			r.Prs[peer].Next = lastLogIndex + 1
		}
		//Q:为啥要对自己的Next和Match做如此处理？because of noop entry
		if peer == r.id {
			r.Prs[peer].Next = lastLogIndex + 2
			r.Prs[peer].Match = lastLogIndex + 1
		}
	}

	// 3.发送带空的entries的AppendEntries的消息
	//上面的初始化：NextIndex=lastLogIndex+1
	//下面做append操作后，lastLogIndex++
	//后续发送时：nextIndex==lastLogIndex，故为空
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: lastLogIndex + 1})
	for _, entry := range r.RaftLog.entries {
		if entry.Index == 0 {
			panic("3.leadersend: entry'idex==0")
		}
	}
	//如果有其他节点，sendAppend
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	//如果只有自己，直接提交
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
		return
	}

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled

// 注：项目屏蔽了消息接收的具体逻辑，所有对消息的处理都在Step()中实现
//来自于其他节点或者上层RawNode
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		//10 basic
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			} else {
				r.bcastRequestVote()
			}
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			//2 extra
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}

	case StateCandidate:
		switch m.MsgType {
		//10 basic
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			} else {
				r.bcastRequestVote()
			}
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			//2 extra
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}

	case StateLeader:
		switch m.MsgType {
		//10 basic
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			//bcast Heartbeat by Leader:
			for peer := range r.Prs {
				if peer != r.id {
					r.sendHeartbeat(peer)
				}
			}
		case pb.MessageType_MsgPropose:
			r.appendEntries(m.Entries)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
			//2 extra
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
// by follower, or (candidate, leader?)
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 1.消息中的任期过期，return false
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, false, None, None)
		return
	}
	//2.如果自己的任期比消息的低，return false 并变为追随者
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		//此处不应该直接发送接收，因为发送接收则意味着和leader的日志已经完全同步
		// r.sendAppendResponse(m.From, true)
		// return
	}
	//2.2或自己是候选者收到任期至少相同的领导者消息，变为追随者
	if m.Term == r.Term && r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
		// r.sendAppendResponse(m.From, true)
		// return
	}
	if m.Term == r.Term && r.Lead == None {
		r.becomeFollower(m.Term, m.From)
	}
	// log.Infof("--+ %d handleAppend, Lead: %d", r.id, r.Lead)

	//3.如果接收者没有能匹配上的leader的日志条目,即prevLogIndex和prevLogTerm的索引任期一样的条目
	//m.Index即prevLogIndex,m.LogTerm即prevLogTerm
	lastLogIndex := r.RaftLog.LastIndex()
	//3.1 如果leader认为的follow的Next大于实际的lastLogIndex, return false
	if lastLogIndex < m.Index {
		r.sendAppendResponse(m.From, false, None, None)
		return
	}
	//3.2 取接收者的log[prevLogIndex]看能否和prevLogTerm能匹配上
	logTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		panic(err)
	}
	//不能匹配：return false
	if logTerm != m.LogTerm {
		r.sendAppendResponse(m.From, false, None, lastLogIndex+1)
		return
	} else {
		//可以匹配上
		if len(m.Entries) > 0 {
			if len(r.RaftLog.entries) == 0 {
				r.RaftLog.apppendNewEntries(m.Entries)
			} else {
				//4.如果已经存在的条目和新条目有冲突(索引相同，任期不同)，则截断后续条目并删除
				conflict, index := r.RaftLog.appendConflict(m.Entries)
				if conflict {
					r.RaftLog.deleteConflictEntries(index)
				}
				//5.删除重复的日志并追加新日志条目
				_, ents := r.RaftLog.deleteRepeatEntries(m.Entries)
				for _, entry := range ents {
					if entry.Index == 0 {
						panic("1.handleappendresponse: entry Index==0")
					}
				}
				if len(ents) > 0 {
					r.RaftLog.apppendNewEntries(ents)
				}
			}

		}
		//6.更新committed
		//如果leader的Commit大于接收者的committed，则取leaderCommit和lastLogIndex的最小值作为更新
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
		}
		last := r.RaftLog.LastIndex()
		// log.Infof("handleApp:%d,len:%d,first:%d,last:%d", r.id, len(r.RaftLog.entries), r.RaftLog.FirstIndex, last)
		lastTerm, _ := r.RaftLog.Term(last)
		r.sendAppendResponse(m.From, true, lastTerm, last)
	}
}

//handleAppendEntriesResponse by leader
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	//1. 如果收到拒绝消息
	if m.Reject {
		//Next-1,重试
		// index := m.Index
		r.Prs[m.From].Next--
		// r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	} else {
		//2. 如果接受到同意
		//更新领导者保存的Prs，节点信息
		if m.Index > r.Prs[m.From].Match {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
			//Q:updateCommit的作用是啥？
			//A:更新leader的committed，并通知跟随者更新
			r.updateCommit()
		}
	}
}

//updateCommit by leader, and then send to follower
func (r *Raft) updateCommit() {
	//1.对所有的Prs.Match进行排序
	// allMatch:=make([]uint64,len(r.Prs))
	//Q:此处若令match为uint64类型的数组，则无法使用sort.Sort
	//A:使用util.uint64Slice类型，已经实现了Sort所需的接口
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		//match = append(match, prs.Match)
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	//2. 找排序后的中位数
	mid := match[(len(match)-1)/2]
	//3.如果中位数大于committed，更新领导者的committed值
	if mid > r.RaftLog.committed {
		logTerm, _ := r.RaftLog.Term(mid)
		// 3.1 如果中位数的日志，任期已经过期
		if logTerm < r.Term {
			return
		}
		//3.2 更新committed，并通知跟随者更新
		r.RaftLog.committed = mid
		// log.Infof("*--+++ %d committed: %d ", r.id, r.RaftLog.committed)
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	//Q:commit部分，为何，如何处理？
	//1. 如果任期过期
	if r.Term > m.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	//2.任期没有过期，重置timeout状态，维护leader的领导
	r.becomeFollower(m.Term, m.From)
	r.sendHeartbeatResponse(m.From, false)
}

//handleHeartbeatResponse by leader
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	//1.如果发现自己任期过期，则变为跟随者
	if m.Reject {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
			return
		}
	} else {
		//2.否则保持领导者状态，检查Match，如果低于自己的LastLogIndex则发送Append同步
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

// handleRequestVote by follower
func (r *Raft) handleRequestVote(m pb.Message) {
	// 按Raft论文的算法描述来
	// log.Infof("--+ %d handleRequestVote: state: %d, Term: %d", r.id, r.State, r.Term)
	//1.任期是否过期，return false
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, false)
		return
	}
	//2 如果消息中的任期更大，成为追随者。但不一定投票（还要看Term和Index)
	if m.Term > r.Term {
		//3.如果候选者日志没有自己新，(先判断Term再判断Index)，return false
		lastLogIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
		if lastLogTerm > m.LogTerm || lastLogTerm == m.LogTerm && lastLogIndex > m.Index {
			r.becomeFollower(m.Term, None)
			r.sendRequestVoteResponse(m.From, false)
			return
		}
		//4.否则，投票给候选者
		//Q: 按照测试的逻辑，此处应该暂时不设候选者为领导者，为什么？
		// r.becomeFollower(m.Term, m.From)
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	if m.Term == r.Term {
		//2.如果已经给别人投过票，return false
		if r.Vote != None && r.Vote != m.From {
			r.sendRequestVoteResponse(m.From, false)
			return
		}
		//3.如果候选者日志没有自己新，(先判断Term再判断Index)，return false
		lastLogIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
		if lastLogTerm > m.LogTerm || lastLogTerm == m.LogTerm && lastLogIndex > m.Index {
			r.sendRequestVoteResponse(m.From, false)
			return
		}

		//4.否则，投票给候选者
		r.becomeFollower(m.Term, None)
		// r.becomeFollower(m.Term, m.From)
		r.Vote = m.From
		r.sendRequestVoteResponse(m.From, true)
	}
}

//handleHeartbeatResponse by candidate
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	//1.统计选票数
	if m.Reject {
		r.VotedReject++
		r.votes[m.From] = false
	} else {
		r.VotedFor++
		r.votes[m.From] = true
	}
	// log.Infof("--++ %d handleVoteResp: state: %d, Term: %d, VotedFor: %d", r.id, r.State, r.Term, r.VotedFor)
	//超过半数就可以决定是当选还是落选
	//2. 过半同意，成为领导者
	if r.VotedFor > uint64(len(r.Prs)/2) {
		// r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader})
		r.becomeLeader()
		return
	}
	//3. 过半反对，退回追随者
	if r.VotedReject > uint64(len(r.Prs)/2) {
		r.becomeFollower(r.Term, None)
	}
}

// leader handlePropose by Sunlly
func (r *Raft) appendEntries(entries []*pb.Entry) {
	ent := make([]*pb.Entry, 0)
	//1.完善entry的信息
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) + 1
		ent = append(ent, entry)
		// log.Infof("*-- Propose appendEntries: %d, index: %d ,State:%s", r.id, entry.Index, r.State)
	}
	//2.添加entry到日志条目
	for _, entry := range ent {
		if entry.Index == 0 {
			panic("2.append: entry Index==0")
		}
	}
	if len(ent) > 0 {
		r.RaftLog.apppendNewEntries(ent)
	}
	//3.更新Prs
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	//4.广播Append消息
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
	//如果有其他节点，sendAppend
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
