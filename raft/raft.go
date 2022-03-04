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
	log.Infof("newRaft:%d", c.ID)
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

//3a using in transferleader by Sunlly
func (r *Raft) sendTimeoutNow(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
	}
	r.msgs = append(r.msgs, msg)
	// msg := pb.Message{
	// 	MsgType: pb.MessageType_MsgAppendResponse,
	// 	To:      to,
	// 	From:    r.id,
	// 	Term:    r.Term,
	// 	Reject:  !success,
	// 	LogTerm: logTerm,
	// 	Index:   logIndex,
	// }
	// r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//Q:为啥需要添加这个条件？是因为Next==0的时候一定是空的entries吗？
	//A: 对于新加入的节点，发送Snapshot
	//sendSnapshot1:新加入节点
	if r.Prs[to].Match == 0 && r.Prs[to].Next == 0 {
		log.Infof("***+++ %d &&&1 sendAppend: to %d", r.id, to)
		r.sendSnapshot(to)
		return false
	}

	nextIndex := r.Prs[to].Next
	//如果不对next==0做判断，此处的prevLogIndex将为负数
	prevLogIndex := nextIndex - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		if err == ErrCompacted {
			//sendSnapshot2:请求的日志已经被压缩，preveLogIndex<l.first
			log.Infof("***+++ %d &&&2 sendAppend: to %d", r.id, to)
			log.Infof("prevLogIndex:%d, r.RaftLog.firstIndex:%d", prevLogIndex, r.RaftLog.FirstIndex)
			r.sendSnapshot(to)
			return false
		}
		panic(err)
	}
	entries := make([]*pb.Entry, 0)
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
	log.Infof("%d sendAppend to %d: entfirst:%d, entlast:%d,len:%d,next:%d,index:%d", r.id, to, r.RaftLog.FirstIndex, r.RaftLog.LastIndex(), len(r.RaftLog.entries), r.Prs[to].Next, msg.Index)
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
	lastLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		//Q:为啥HeartbearResponse会考虑reject?
		//A：每个消息都带有任期信息，如果接收者发现m.Term>r.Term，
		//直接变为跟随者，并在response消息中附上Reject=true
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: lastLogTerm,
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

	log.Infof("++ %d becomeCadidate", r.id)
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
	log.Infof("+++ %d becomeLeader", r.id)
	//1. 状态转换
	if r.State != StateLeader {
		r.resetTimeout()
		r.State = StateLeader
		r.Lead = r.id
		r.leadTransferee = None
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
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
			if _, ok := r.Prs[r.id]; ok {
				r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			}
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
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
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
			if r.leadTransferee == None {
				r.appendEntries(m.Entries)
			}

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
			r.handleTransferLeader(m)
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
		// log.Infof("&&&1: %d sendAppendResponse:", r.id)
		// log.Infof("++---- %d handleAppend: r.Term:%d , m.Term:%d,", r.id, r.Term, m.Term)
		r.sendAppendResponse(m.From, false, None, None)
		return
	}
	//2.如果自己的任期比消息的低，return false 并变为追随者
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		//此处不应该直接发送接收，因为发送接收则意味着和leader的日志已经完全同步

	}
	//2.2或自己是候选者收到任期至少相同的领导者消息，变为追随者
	if m.Term == r.Term && r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	}
	if m.Term == r.Term && r.Lead == None {
		r.becomeFollower(m.Term, m.From)
	}
	// log.Infof("--+ %d handleAppend, Lead: %d", r.id, r.Lead)

	//3.如果接收者没有能匹配上的leader的日志条目,即prevLogIndex和prevLogTerm的索引任期一样的条目
	//m.Index即prevLogIndex,m.LogTerm即prevLogTerm
	lastLogIndex := r.RaftLog.LastIndex()
	lastEntIndex := m.Index + uint64(len(m.Entries))

	//3.1 如果leader认为的follow的Next大于实际的lastLogIndex, return false
	if lastLogIndex < m.Index {
		log.Infof("&&&2: %d sendAppenResponse:", r.id)
		log.Infof("++---- %d handleAppend: first:%d last:%d, len:%d, m.Index:%d, lenAppend:%d, r.Term:%d, m.Term:%d", r.id, r.RaftLog.FirstIndex, r.RaftLog.LastIndex(), len(r.RaftLog.entries), m.Index, len(m.Entries), r.Term, m.Term)
		r.sendAppendResponse(m.From, false, None, lastLogIndex+1)
		return
	}
	//****增加条件，避免对多余的append做处理
	// if r.RaftLog.FirstIndex > m.Index && (len(r.RaftLog.entries) > 0 || !IsEmptySnap(r.RaftLog.pendingSnapshot)) {
	// 	return
	// }
	if r.RaftLog.FirstIndex <= m.Index {
		log.Infof("++---- %d handleAppend: first:%d last:%d, len:%d, m.Index:%d, lenAppend:%d", r.id, r.RaftLog.FirstIndex, r.RaftLog.LastIndex(), len(r.RaftLog.entries), m.Index, len(m.Entries))
		//3.2 取接收者的log[prevLogIndex]看能否和prevLogTerm能匹配上
		logTerm, err := r.RaftLog.Term(m.Index)
		log.Infof("++---- %d handleAppend: logTerm:%d, m.LogTerm:%d", r.id, logTerm, m.LogTerm)
		if err != nil {
			log.Infof("***panic: Term:%d", m.Index)
			panic(err)
		}
		//不能匹配：return false
		if logTerm != m.LogTerm {
			// log.Infof("&&&3: sendAppenResponse:")
			// log.Infof("++---- %d handleAppend: logTerm:%d, m.LogTerm:%d", r.id, logTerm, m.LogTerm)
			r.sendAppendResponse(m.From, false, None, lastLogIndex+1)
			return
		}
	}
	//如果firstIndex>m.Index：恰好entries的开头为firstindex
	//如果firstIndex>（m.Index+1）：截断前面的日志，避免后续可能造成的问题
	if r.RaftLog.FirstIndex > m.Index+1 {
		if uint64(len(m.Entries)) <= (r.RaftLog.FirstIndex - (m.Index + 1)) {
			m.Entries = make([]*pb.Entry, 0)
		} else {
			m.Entries = m.Entries[r.RaftLog.FirstIndex-(m.Index+1):]
		}
	}
	// log.Infof("++---- %d handleAppend: first:%d last:%d, len:%d, m.Index:%d, lenAppend:%d", r.id, r.RaftLog.FirstIndex, r.RaftLog.LastIndex(), len(r.RaftLog.entries), m.Index, len(m.Entries))
	// // log.Infof("***commit:%d", r.RaftLog.committed)
	// // log.Infof("***applied:%d", r.RaftLog.applied)
	// //3.2 取接收者的log[prevLogIndex]看能否和prevLogTerm能匹配上
	// logTerm, err := r.RaftLog.Term(m.Index)
	// log.Infof("++---- %d handleAppend: logTerm:%d, m.LogTerm:%d", r.id, logTerm, m.LogTerm)
	// if err != nil {
	// 	log.Infof("***panic: Term:%d", m.Index)
	// 	panic(err)
	// }
	// //不能匹配：return false
	// if logTerm != m.LogTerm {
	// 	// log.Infof("&&&3: sendAppenResponse:")
	// 	// log.Infof("++---- %d handleAppend: logTerm:%d, m.LogTerm:%d", r.id, logTerm, m.LogTerm)
	// 	r.sendAppendResponse(m.From, false, None, lastLogIndex+1)
	// 	return
	// } else {
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
			if len(ents) > 0 {
				r.RaftLog.apppendNewEntries(ents)
			}
		}

	}
	//6.更新committed
	//如果leader的Commit大于接收者的committed，则取leaderCommit和lastLogIndex的最小值作为更新
	if m.Commit > r.RaftLog.committed {
		if lastEntIndex >= r.RaftLog.FirstIndex {
			r.RaftLog.committed = min(m.Commit, lastEntIndex)
		}
	}
	last := r.RaftLog.LastIndex()
	// log.Infof("handleApp:%d,len:%d,first:%d,last:%d", r.id, len(r.RaftLog.entries), r.RaftLog.FirstIndex, last)
	lastTerm, err := r.RaftLog.Term(last)
	if err != nil {
		panic(err)
	}
	log.Infof("&&&4: %d sendAppenResponse:", r.id)
	r.sendAppendResponse(m.From, true, lastTerm, last)
	// }
}

//handleAppendEntriesResponse by leader
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		// log.Infof("&&&Term1: handleAppenResponse: Reject")
		// log.Infof("+++---- %d handleAR: from: %d , m.Term:%d, r.Term:%d", r.id, m.From, m.Term, r.Term)
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Term < r.Term {
		// log.Infof("&&&Term2: handleAppenResponse: Reject")
		// log.Infof("+++---- %d handleAR: from: %d , m.Term:%d, r.Term:%d", r.id, m.From, m.Term, r.Term)
		return
	}
	//1. 如果收到拒绝消息
	if m.Reject {
		//Next-1,重试
		// index := m.Index
		log.Infof("&&&0: %d handleAppenResponse: Reject, Form:%d", r.id, m.From)
		log.Infof("+++---- %d handleAR: from: %d ,", r.id, m.From)
		r.Prs[m.From].Next--
		// log.Infof("***%d commit:%d", r.id, r.RaftLog.committed)
		// r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	} else {
		//2. 如果接受到同意
		//更新领导者保存的Prs，节点信息
		if m.Index > r.Prs[m.From].Match {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
			log.Infof("%d handleAP %d Response:match:%d, next:%d", r.id, m.From, m.Index, m.Index+1)
			//Q:updateCommit的作用是啥？
			//A:更新leader的committed，并通知跟随者更新
			r.updateCommit()
		}

		if r.leadTransferee == m.From {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: m.From, To: r.id})
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
		// lastLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		// if m.LogTerm < lastLogTerm || m.LogTerm == lastLogTerm && m.Index < r.RaftLog.LastIndex() {
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
		log.Infof("*-- %d Propose appendEntries: , index: %d ,State:%s", r.id, entry.Index, r.State)
	}
	//2.添加entry到日志条目
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

//2c by Sunlly0
func (r *Raft) sendSnapshot(to uint64) {
	log.Infof("***+++ %d sendSnapshot: to %d", r.id, to)
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		log.Infof("%d sendSnapshot:failed", r.id)
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	log.Infof("%d sendSnapshot:success", r.id)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	log.Infof("****+++ %d handleSnapshot:From %d", r.id, m.From)
	meta := m.Snapshot.Metadata
	//1.判断快照是否过期
	if meta.Index <= r.RaftLog.committed {
		log.Infof(" %d handleSnapshot:out of date", r.id)
		r.sendAppendResponse(m.From, true, None, r.RaftLog.committed)
		return
	}
	//2.更新自身状态
	r.becomeFollower(max(r.Term, m.Term), m.From)
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.FirstIndex = meta.Index + 1
	r.RaftLog.stabled = meta.Index
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	//3.保存快照，以便后续的应用
	r.RaftLog.pendingSnapshot = m.Snapshot
	//4. 回复appendResponse
	log.Infof(" %d handleSnapshot:success", r.id)
	r.sendAppendResponse(m.From, true, None, r.RaftLog.LastIndex())
}

//3a by Sunlly
func (r *Raft) handleTransferLeader(m pb.Message) {
	//1.检查leader是否能进行本次转移
	//如果目前有其他节点在转移，则忽略本次消息
	log.Infof("%d handleTransferleader: to %d, leadTransferee:%d", r.id, m.From, r.leadTransferee)
	// if r.leadTransferee != None && r.leadTransferee != m.From {
	// 	return
	// }
	if _, ok := r.Prs[m.From]; ok {
		r.leadTransferee = m.From
		//2.检查目标节点的日志新旧，如果新则继续，如果旧则发append同步
		if r.Prs[m.From].Match != r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
			return
		}
		//3.发送timeoueNow消息，使被转移节点触发选举，此刻一定能当选
		r.resetTimeout()
		r.sendTimeoutNow(m.From)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		log.Infof("%d addNode%d", r.id, id)
		r.Prs[id] = &Progress{Next: 0}
		r.PendingConfIndex = None
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	//1.删除自己
	//2.删除其他节点
	if _, ok := r.Prs[id]; ok {
		log.Infof("%d removeNode%d", r.id, id)
		delete(r.Prs, id)
		//2.1如果自己的状态是领导者，需要验证是否在删除节点后有可提交的日志
		if r.State == StateLeader {
			r.updateCommit()
		}
	}
	r.PendingConfIndex = None
}
