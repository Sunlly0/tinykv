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
	VotedFor    uint64
	VotedReject uint64

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
	lastLogIndex := r.RaftLog.LastIndex()

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
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	entries:=[]*pb.Entry
	prevLogIndex:=r.Prs[to].Next-1
	prevLogTerm,_:=r.RaftLog.Term(prevLogIndex)

	r.RaftLog.Term(r.RaftLog.LastIndex())
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit: r.RaftLog.committed,
		LogTerm: prevLogTerm,
		Index:prevLogIndex,
		Entries:entries,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendAppendResponse(to uint64,success bool)  {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject: !success,
		//LogTerm: prevLogTerm,
		//Index:prevLogIndex,
	}
	r.msgs = append(r.msgs, msg)
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
		//Q:为啥Heartbear会考虑commit,何为commit?
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
	r.electionElapsed = -rand.Intn(r.electionTimeout)

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	//1.时间增加
	//2.判断是否超时
	switch r.State {
	//2.1 Follower、candidate选举超时，处理：变成候选者，重新选举
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.resetTimeout()
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
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	//1.状态更新为Candidate，任期++，自己给自己投票
	r.State = StateCandidate
	r.Term++
	r.votes = make(map[uint64]bool, 0)
	r.Vote = r.id
	r.votes[r.id] = true

	//2.发送选举信息
	// 2.1 如果peers只有自己，直接当选
	if len(r.Prs) == 1 {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader})
	}
	//2.2 发送选举消息给除自己以外所有的peers
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
	if r.State != StateLeader {
		r.resetTimeout()
		r.State = StateLeader
		r.Lead = r.id
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled

// 注：项目屏蔽了消息接收的具体逻辑，所有对消息的处理都在Step()中实现
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		//10 basic
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
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
			r.becomeLeader()
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
		case pb.MessageType_MsgAppend:
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
	//1.候选者接收到领导者的Append消息
	if r.State==StateCandidate{
		//1.1 消息所包含任期更高，变为追随者
		if m.Term>r.Term{
			r.becomeFollower(m.Term,m.From)
			r.sendAppendResponse(m.From, true)
		}
		//1.2 自己任期高，拒绝
		if m.Term<r.Term{
			r.sendAppendResponse(m.From, false)
		}
		return
	}

	//2.跟随者.
	//2.1任期是否过期，return false
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, false)
	}
	//2.2 日志匹配不上，return false和自己的日志号，供leader参考匹配
	else if r.RaftLog.LastIndex()<m.Index{
		r.sendAppendResponse(m.From,false)
	}else{
		//接收
		r.sendAppendResponse(m.From,true)
	}


}

//handleAppendEntriesResponse by leader
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

//handleHeartbeatResponse by leader
func (r *Raft) handleHeartbeatResponse(m pb.Message) {

}

// handleRequestVote by follower
func (r *Raft) handleRequestVote(m pb.Message) {
	// 按Raft论文的算法描述来
	//1.任期是否过期，return false
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, false)
	}
	//2.如果已经给别人投过票，return false
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, false)
	}
	//3.如果候选者日志没有自己新，(先判断Term再判断Index)，return false
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	if lastLogTerm < m.LogTerm || lastLogTerm == m.LogTerm && lastLogIndex < m.Index {
		r.sendRequestVoteResponse(m.From, false)
	}
	//4.否则，投票给候选者
	r.becomeFollower(m.Term, m.From)
	r.Vote = m.From
	r.sendRequestVoteResponse(m.From, true)
}

//handleHeartbeatResponse by candidate
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	//统计选票数
	if m.Reject {
		r.VotedReject++
	} else {
		r.VotedFor++
	}
	//超过半数就可以决定是当选还是落选
	if r.VotedFor > uint64(len(r.Prs)/2) {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader})
	}
	if r.VotedReject > uint64(len(r.Prs)/2) {
		r.becomeFollower(r.Term, None)
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
