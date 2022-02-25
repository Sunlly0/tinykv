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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	//Q:添加该数据结构的必要性在哪？
	//A:因为快照压缩后，entries[0]的索引不一定是0，所以需要记录起始位置的索引
	FirstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//Q:为啥newLog要这样初始化？
	//A: 将storage中的条目加载到Log中。对于未持久化的条目必须加载，对于已持久化的条目（已应用），加载会为后续带来方便。
	//lo，即storage.FirstIndex
	lo, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()

	//if err !=nil{
	// 	panic(err)
	// }
	entries, err := storage.Entries(lo, hi+1)
	if err != nil {
		log.Panic(err)
	}

	//A:关于此处为何这样初始化，参考RaftLog的结构
	//  snapshot/first.....applied....committed....stabled.....last
	//  --------|------------------------------------------------|
	//                            log entries
	l := &RaftLog{
		storage:    storage,
		entries:    entries,
		committed:  lo - 1,
		applied:    lo - 1,
		stabled:    hi,
		FirstIndex: lo,
	}
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		// log.Infof("unstableEnt: stabled:%d, last:%d", l.stabled, l.LastIndex())
		// for _, entry := range l.entries {
		// 	if entry.Index == 0 {
		// 		log.Infof("!!!unstableEnt index==0: stabled:%d, last:%d", l.stabled, l.LastIndex())
		// 		log.Infof("len:%d", len(l.entries))
		// 		for i, ent := range l.entries {
		// 			log.Infof("index==0: entunm: %d, index:%d", i, ent.Index)
		// 		}
		// 		panic("unstableEnts: entry Index == 0")
		// 	}
		// }
		return l.entries[l.stabled-l.FirstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		// log.Infof("nextEnts: applied:%d, commit:%d, stabled:%d,last:%d FirstIndex:%d", l.applied, l.committed, l.stabled, l.LastIndex(), l.FirstIndex)
		// for _, entry := range l.entries {
		// 	if entry.Index == 0 {
		// 		panic("append: entry Index == 0")
		// 	}
		// }
		//Q:committed切片是否+1？
		//A:需要+1
		return l.entries[l.applied-l.FirstIndex+1 : l.committed-l.FirstIndex+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//1.如果有entries
	if len(l.entries) > 0 {
		// log.Infof("LastIndex: len:%d,last:%d", len(l.entries), l.entries[len(l.entries)-1].Index)
		return l.entries[len(l.entries)-1].Index
	}
	//2.如果没有entries
	if len(l.entries) == 0 {
		index, _ := l.storage.LastIndex()
		if index > 0 {

			return index
		}
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	//考虑FirstIndex
	// if i < l.FirstIndex {
	// 	return 0, ErrCompacted
	// }
	// in entries
	if len(l.entries) > 0 && i >= l.FirstIndex {
		// log.Infof("Term: firstindex:%d,last:%d ,index:%d, len:%d", l.FirstIndex, l.LastIndex(), i, len(l.entries))
		// for i, entry := range l.entries {
		// 	log.Infof("Term:entnum:%d,index:%d", i, entry.Index)
		// }
		return l.entries[i-l.FirstIndex].Term, nil
	}
	// not in entries
	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

//extra funcs by Sunlly
func (l *RaftLog) Slice(begin uint64, end uint64) []*pb.Entry {
	var slice []*pb.Entry
	for i := begin - l.FirstIndex; i <= end-l.FirstIndex; i++ {
		entry := l.entries[i]
		slice = append(slice, &entry)
	}
	// for _, entry := range slice {
	// 	if entry.Index == 0 {
	// 		panic("Slice: entry Index == 0")
	// 	}
	// }
	return slice
}

//返回是否冲突，冲突的index
func (l *RaftLog) appendConflict(entries []*pb.Entry) (bool, uint64) {
	var index uint64
	for _, entry := range entries {
		if entry.Index < l.FirstIndex {
			continue
		}
		if entry.Index <= l.LastIndex() {
			logTerm, err := l.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			//如果已经存在的条目和新条目有冲突(索引相同，任期不同)，
			if logTerm != entry.Term {
				index = entry.Index
				return true, index
			}
		}
	}

	return false, index
}

//删除冲突的日志：从index开始往后的都删除
func (l *RaftLog) deleteConflictEntries(index uint64) {
	// for _, entry := range l.entries {
	// // 	if entry.Index == 0 {
	// // 		panic("1.1 deleteConflictEntries: entry Index == 0")
	// // 	}
	// // }
	// l.entries = l.entries[:index-1]
	l.entries = l.entries[0 : index-l.FirstIndex]
	//注：需要更新stabled状态，新增的日志都是不稳定的了
	l.stabled = min(index-1, l.stabled)
}

//删除不冲突情况下，和自己已有日志重复的新日志，返回删除后的日志
func (l *RaftLog) deleteRepeatEntries(entries []*pb.Entry) (bool, []*pb.Entry) {
	//0.如果没有日志，则不作处理
	if len(l.entries) == 0 {
		return false, entries
	}
	//1.如果新来的日志的第一个的Index就比自己大，则没有重复的日志
	if entries[0].Index > l.LastIndex() {
		return false, entries
	}
	//2.否则有重复日志。删除日志中所有小于等于lastIndex的日志，并返回
	for i, entry := range entries {
		if entry.Index > l.LastIndex() {
			return true, entries[i:]
		}
	}
	//3.如果所有日志都重复，则全部删除，返回空
	return true, make([]*pb.Entry, 0)
}

//添加新日志条目，默认日志中已经有Term和Index等信息
func (l *RaftLog) apppendNewEntries(entries []*pb.Entry) {
	for _, entry := range entries {
		l.entries = append(l.entries, *entry)
	}
}
