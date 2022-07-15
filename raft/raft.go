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
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

const DEBUG = false

func LogPrint(format string, logLevel log.LogLevel, v ...interface{}) {
	log.SetLevel(logLevel)
	if DEBUG {
		switch logLevel {
		case log.LOG_LEVEL_INFO:
			log.Infof(format, v...)
		case log.LOG_LEVEL_WARN:
			log.Warnf(format, v...)
		case log.LOG_LEVEL_DEBUG:
			log.Debugf(format, v...)
		case log.LOG_LEVEL_ERROR:
			log.Errorf(format, v...)
		case log.LOG_LEVEL_FATAL:
			log.Fatalf(format, v...)
		}
	}
	log.SetLevel(log.LOG_LEVEL_INFO)
	return
}

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

	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

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
	raftLog := newLog(c.Storage)
	hardState, confState, err1 := c.Storage.InitialState()
	if err1 != nil {
		panic(err1)
	}
	r := &Raft{
		id:      c.ID,
		RaftLog: raftLog,
		Prs:     map[uint64]*Progress{},
		votes:   map[uint64]bool{},
		//msgs:             make([]pb.Message, 0), msgs不用初始化
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	nodes := confState.GetNodes()
	if c.peers == nil {
		c.peers = nodes
	}
	if c.Applied > 0 {
		raftLog.applied = c.Applied
	}
	lastLogIndex := r.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer != r.id {
			r.Prs[peer] = &Progress{
				Match: 0,
				Next:  lastLogIndex + 1,
			}
		} else {
			r.Prs[peer] = &Progress{
				Match: lastLogIndex,
				Next:  lastLogIndex + 1,
			}
		}
	}
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.becomeFollower(0, None)
	r.Vote, r.Term, r.RaftLog.committed = hardState.GetVote(), hardState.GetTerm(), hardState.GetCommit()
	return r
}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	m := pb.Message{}
	m.To = to
	var err error
	m.LogTerm, err = r.RaftLog.Term(pr.Next - 1)
	if err == nil {
		var term uint64
		term, _ = r.RaftLog.Term(pr.Next - 1)
		m.Index = pr.Next - 1
		m.LogTerm = term
		//ents := r.RaftLog.nextEnts()
		ents := r.RaftLog.entries
		if len(ents) == 0 {
			return false
		}
		var ents1 []*pb.Entry
		n := uint64(len(ents))
		for i := m.Index - r.RaftLog.firstIndex + 1; i < n; i++ {
			ents1 = append(ents1, &ents[i])
		}
		m.MsgType = pb.MessageType_MsgAppend
		m.Term = r.Term
		m.Entries = ents1
		m.From = r.id
		m.Commit = r.RaftLog.committed
	} else if err == ErrCompacted {
		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Debugf("%d failed to send snapshot to %d because snapshot is temporally unavailable", r.id, to)
				return false
			}
			panic(err)
		}
		m.Snapshot = &snapshot
		m.From = r.id
	}
	r.msgs = append(r.msgs, m)
	//log.Infof("leader %d sends append[index:%d term:%d] to node %d ", r.id, m.Index, m.Term, m.To)
	LogPrint("leader %d sends append[index:%d term:%d] to node %d ", log.LOG_LEVEL_DEBUG, r.id, m.Index, m.Term, m.To)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, index uint64, reject bool, logTerm uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   index,
		Reject:  reject,
	})
}

func (r *Raft) sendRequestVote(to uint64) {
	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	//log.Infof("node %d sends a requestVote[logTerm:%d, index:%d] to node %d.",
	//	r.id, logTerm, r.RaftLog.LastIndex(), to)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   r.RaftLog.LastIndex(),
	})
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	//commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		//Commit:  commit,
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) sendHeartBeatResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Reject:  reject,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			if err := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:      r.id,
				From:    r.id,
			}); err != nil {
				print(err)
			}
		}
	} else {
		//log.Info("tick tick tick")
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			if err := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			}); err != nil {
				print(err)
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = None
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Lead = lead
	r.State = StateFollower
	r.votes = map[uint64]bool{}
	//log.Infof("%x 成为 follower 在任期 %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
	r.State = StateCandidate
	//log.Infof("%x 成为 candidate 在任期 %d", r.id, r.Term)
	LogPrint("%d becomes candidate at term %d", log.LOG_LEVEL_WARN, r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	//log.Infof("%x 成为 leader 在任期 %d", r.id, r.Term)
	LogPrint("%d becomes leader at term %d", log.LOG_LEVEL_WARN, r.id, r.Term)
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.Lead = r.id
	lastIndex := r.RaftLog.LastIndex()
	for k := range r.Prs {
		if k == r.id {
			r.Prs[k].Match = lastIndex + 1
			r.Prs[k].Next = lastIndex + 2
		} else {
			r.Prs[k].Next = lastIndex + 1
			r.Prs[k].Match = 0
		}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: lastIndex + 1,
	})
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok && m.MsgType == pb.MessageType_MsgTimeoutNow {
		return nil
	}
	if m.Term > r.Term {
		r.leadTransferee = None
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		err := r.stepFollower(m)
		if err != nil {
			return err
		}
	case StateCandidate:
		err := r.stepCandidate(m)
		if err != nil {
			return err
		}
	case StateLeader:
		err := r.stepLeader(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.brstHeartBeat()
	case pb.MessageType_MsgHeartbeatResponse:
		if r.Prs[m.From].Match < r.RaftLog.committed {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None {
			r.handleMsgPropose(m)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHup:
		//log.Infof("node %d received msgHup from %d", r.id, m.From)
		r.compaign()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		//log.Infof("node %d received msgTimeoutNow", r.id)
		r.compaign()
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHup:
		//log.Infof("node %d received msgHup", r.id)
		r.compaign()
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleMsgRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}

// compaign makes an election
func (r *Raft) compaign() {
	//log.Infof("node %d is doing an election", r.id)
	if r.State == StateLeader {
		return
	} else {
		r.becomeCandidate()
		if len(r.Prs) == 1 {
			r.becomeLeader()
			return
		}
		// broadcast MsgRequestVote
		for k := range r.Prs {
			if k == r.id {
				continue
			}
			r.sendRequestVote(k)
		}
		return
	}
}

func (r *Raft) brstHeartBeat() {
	for k := range r.Prs {
		if k == r.id {
			continue
		}
		r.sendHeartbeat(k)
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	//log.Infof("leader %d is handling MsgPropose[index=%d type:]", r.id, m.Index, m.Entries[0].EntryType)
	lastIndex := r.RaftLog.LastIndex()
	for i, ent := range m.Entries {
		ent.Term = r.Term
		ent.Index = lastIndex + uint64(i) + 1
		if ent.EntryType == pb.EntryType_EntryConfChange {
			//if r.PendingConfIndex != None {
			//	continue
			//}
			if r.PendingConfIndex > r.RaftLog.applied {
				continue
			}
			r.PendingConfIndex = ent.Index
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		//log.Infof("leader %d appends entry[index:%d, term:%d, data:%s]", r.id, ent.Index, ent.Term, ent.Data)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	for k := range r.Prs {
		if k != r.id {
			r.sendAppend(k)
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if r.Term <= m.Term {
		if r.Vote == None || r.Vote == m.From {
			lastIndex := r.RaftLog.LastIndex()
			lastLogTerm, _ := r.RaftLog.Term(lastIndex)
			if lastLogTerm <= m.LogTerm {
				if lastLogTerm < m.LogTerm || lastIndex <= m.Index {
					r.Vote = m.From
					r.electionElapsed = 0
					//log.Infof("node %d vote for node %d", r.id, m.From)
					r.sendRequestVoteResponse(m.From, false)
					return
				}
			}
		}
	}
	r.sendRequestVoteResponse(m.From, true)
}

func (r *Raft) handleMsgRequestVoteResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	r.votes[m.From] = !m.Reject
	var cnt int
	for _, vote := range r.votes {
		if vote {
			cnt++
		}
	}
	size := len(r.Prs)
	if cnt > size/2 {
		if r.State == StateCandidate {
			r.becomeLeader()
		}
	} else if len(r.votes)-cnt > size/2 {
		r.becomeFollower(m.Term, None)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Reply false if term < currentTerm (§5.1)
	//log.Infof("node [%d] received append[index:%d, term:%d, logTerm:%v] from leader %d", r.id, m.Index, m.Term, m.LogTerm, m.From)
	LogPrint("node [%d] received append[index:%d, term:%d, logTerm:%v] from leader %d", log.LOG_LEVEL_DEBUG, r.id, m.Index, m.Term, m.LogTerm, m.From)
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, r.RaftLog.committed, true, None)
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.electionElapsed = 0
	rl := r.RaftLog
	lastIndex := rl.LastIndex()
	//log.Infof("node [%d] lastIndex:%v", m.To, lastIndex)
	if lastIndex < m.Index {
		//r.sendAppendResponse(m.From, lastIndex, true, None)
		// AppendResponse的index与etcd中的hintindex类似
		r.sendAppendResponse(m.From, lastIndex+1, true, None)
		return
	}
	if !rl.matchTerm(m.Index, m.LogTerm) {
		//Reply false if log doesn’t contain an entry at prevLogIndex
		//whose term matches prevLogTerm (§5.3)
		logTerm, _ := rl.Term(m.Index)
		sliceIndex := sort.Search(int(m.Index-rl.firstIndex+1), func(i int) bool {
			return rl.entries[i].Term == logTerm
		})
		index := uint64(sliceIndex) + rl.firstIndex
		r.sendAppendResponse(m.From, index, true, logTerm)
		//r.sendAppendResponse(m.From, min(lastIndex, m.Index), true, m.LogTerm)
		return
	} else {
		lastnewi := m.Index + uint64(len(m.Entries))
		//find conflicts
		//If an existing entry conflicts with a new one (same index
		//but different terms), delete the existing entry and all that
		//follow it (§5.3)
		for i, entry := range m.Entries {
			if entry.Index < rl.firstIndex {
				continue
			}
			if entry.Index <= rl.LastIndex() {
				if !rl.matchTerm(entry.Index, entry.Term) {
					idx := entry.Index - rl.firstIndex
					rl.entries[idx] = *entry
					rl.entries = rl.entries[:idx+1]
					rl.stabled = min(rl.stabled, entry.Index-1)
					//n := len(m.Entries)
					//for j := i + 1; j < n; j++ {
					//	rl.entries = append(rl.entries, *m.Entries[j])
					//}
					//rl.entries = rl.entries[:len(rl.entries)]
				}
			} else {
				n := len(m.Entries)
				for j := i; j < n; j++ {
					rl.entries = append(rl.entries, *m.Entries[j])
				}
				break
			}
		}
		//If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		if m.Commit > rl.committed {
			rl.committed = min(m.Commit, lastnewi)
		}
		r.sendAppendResponse(m.From, rl.LastIndex(), false, None)
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	//log.Infof("node [%d] received an append response[reject:%v index:%v] from node [%d]", r.id, m.Reject, m.Index, m.From)
	LogPrint("node [%d] received an append response[reject:%v index:%v logTerm:%v] from node [%d]", log.LOG_LEVEL_DEBUG, r.id, m.Reject, m.Index, m.LogTerm, m.From)
	if m.Reject {
		//log.Infof("node [%d] next:%v match:%v", m.From, r.Prs[m.From].Next, r.Prs[m.From].Match)
		index := m.Index
		if m.LogTerm > 0 {
			sliceIndex := sort.Search(len(r.RaftLog.entries), func(i int) bool {
				return r.RaftLog.entries[i].Term > m.LogTerm
			})
			if sliceIndex > 0 && r.RaftLog.entries[sliceIndex-1].Term == m.LogTerm {
				index = uint64(sliceIndex) + r.RaftLog.firstIndex
			}
		}
		r.Prs[m.From].Next = index
		//r.Prs[m.From].Next--
		r.sendAppend(m.From)
	} else {
		term, _ := r.RaftLog.Term(m.Index)
		if term != r.Term || m.Index < r.Prs[m.From].Next {
			return
		}
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		if m.From == r.leadTransferee && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(m.From)
			r.leadTransferee = None
		}
		r.LeaderCommit()
	}
}

//func (r *Raft) leaderCommit(m pb.Message) {
//	if m.Index > r.RaftLog.committed {
//		for r.RaftLog.committed < r.RaftLog.LastIndex() {
//			newCommit := r.RaftLog.committed + 1
//			cnt := 1
//			for k := range r.Prs {
//				if k == r.id {
//					continue
//				} else {
//					if r.Prs[k].Match >= newCommit {
//						cnt++
//					}
//				}
//			}
//			if cnt > len(r.Prs)/2 {
//				//log.Infof("leader %d committed[index:%d].", r.id, r.RaftLog.committed+1)
//				r.RaftLog.committed++
//				for k := range r.Prs {
//					if k == r.id {
//						continue
//					}
//					r.sendAppend(k)
//				}
//			} else {
//				break
//			}
//		}
//	}
//}

func (r *Raft) LeaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]
	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			//log.Infof("leader %d committed[index:%d].", r.id, r.RaftLog.committed)
			for k := range r.Prs {
				if k == r.id {
					continue
				}
				r.sendAppend(k)
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term {
		r.sendHeartBeatResponse(m.From, true)
		return
	}
	//log.Infof("node %d received a heartbeat", r.id)
	r.becomeFollower(m.Term, m.From)
	//r.electionElapsed = 0
	//r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendHeartBeatResponse(m.From, false)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	sindex := meta.Index
	if sindex <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, r.RaftLog.committed, false, None)
		return
	}
	r.becomeFollower(max(r.Term, m.Term), m.From)
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	r.RaftLog.firstIndex = sindex + 1
	r.RaftLog.committed = sindex
	r.RaftLog.stabled = sindex
	r.RaftLog.applied = sindex
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	r.sendAppendResponse(m.From, r.RaftLog.LastIndex(), false, None)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	//log.Infof("node %d add node %d", r.id, id)
	LogPrint("node %d add node %d", log.LOG_LEVEL_WARN, r.id, id)
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: 1}
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			r.LeaderCommit()
		}
	}
	r.PendingConfIndex = None
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	leadTransferee := m.From
	if leadTransferee == r.id {
		return
	}
	if r.leadTransferee != None && r.leadTransferee == leadTransferee {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.electionElapsed = 0
	r.leadTransferee = leadTransferee
	if r.Prs[leadTransferee].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(leadTransferee)
		//log.Infof("node %d is sending timeoutNow to node %d", r.id, m.From)
	} else {
		r.sendAppend(leadTransferee)
	}
}

func (r *Raft) sendTimeoutNow(transferee uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      transferee,
		From:    r.id,
	})
}
