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
	// firstIndex
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	fstInd, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lstInd, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(fstInd, lstInd+1)
	if err != nil {
		panic(err)
	}
	resLog := &RaftLog{
		storage:    storage,
		committed:  fstInd - 1,
		applied:    fstInd - 1,
		stabled:    lstInd,
		firstIndex: fstInd,
		entries:    entries,
	}
	return resLog
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
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//off := max(l.applied+1, l.firstIndex)
	//if len(l.entries) > 0 {
	//	if l.committed+1 > off {
	//		return l.entries[off : l.committed-l.firstIndex+1]
	//	}
	//}
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.firstIndex+1 : l.committed-l.firstIndex+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		return 0
	}
	return lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	//log.Infof("now i is %d firstIndex is %d l.entries len is %d", i, l.firstIndex, len(l.entries))
	if i > l.LastIndex() {
		return 0, nil
	}
	if len(l.entries) > 0 && i >= l.firstIndex && i <= l.LastIndex() {
		return l.entries[i-l.firstIndex].Term, nil
	}
	//term, err := l.storage.Term(i)
	//if err != nil {
	//	return 0, err
	//}
	//if term == 0 {
	//	if len(l.entries) > 0 {
	//		return l.entries[i].Term, nil
	//	}
	//}
	return l.storage.Term(i)
}

// matchTern return if term of i equals term
func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	//log.Infof("term is:%d", t)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) RemoveEntriesAfter(lo uint64) {
	l.stabled = min(l.stabled, lo-1)
	if lo-l.firstIndex >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:lo-l.firstIndex]
}
