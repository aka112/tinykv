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

	logger *log.Logger

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
		id:                        c.ID,
		Term:                      hardState.GetTerm(),
		Vote:                      hardState.GetVote(),
		RaftLog:                   raftLog,
		Prs:                       map[uint64]*Progress{},
		State:                     StateFollower,
		votes:                     map[uint64]bool{},
		msgs:                      []pb.Message{},
		Lead:                      None,
		heartbeatTimeout:          c.HeartbeatTick,
		electionTimeout:           c.ElectionTick,
		randomizedElectionTimeout: c.ElectionTick,
		electionElapsed:           0,
		heartbeatElapsed:          0,
		logger:                    log.New(),
	}
	nodes := confState.GetNodes()
	if c.peers == nil {
		c.peers = nodes
	}
	if c.Applied > 0 {
		if c.Applied >= raftLog.applied && c.Applied <= raftLog.committed {
			raftLog.applied = c.Applied
		}
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
	r.becomeFollower(r.Term, None)
	r.RaftLog.committed = hardState.GetCommit()
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	m := pb.Message{}
	m.To = to
	term, _ := r.RaftLog.Term(pr.Next - 1)
	ents := r.RaftLog.nextEnts()
	if len(ents) == 0 {
		return false
	}
	var ents1 []*pb.Entry
	for _, val := range ents {
		ents1 = append(ents1, &val)
	}
	m.MsgType = pb.MessageType_MsgAppend
	m.Index = pr.Next - 1
	m.LogTerm = term
	m.Entries = ents1
	m.Commit = r.RaftLog.committed
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  commit,
	}
	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.logger.Info("Reached heartbeatTimeout")
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
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.logger.Infof("Reached electionTimeout, %x starts a election", r.id)
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
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
	r.State = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.Lead = r.id
	for k := range r.Prs {
		r.Prs[k].Next = r.Prs[k].Match + 1
	}
	err := r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Entries: nil,
	})
	if err != nil {
		return
	}
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
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
		r.handleMsgBeat()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
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
		r.handleMsgHup()
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleMsgRequestVoteResponse(m)
	}
	return nil
}

// handleMsgHup handle MsgHup
func (r *Raft) handleMsgHup() {
	if r.State == StateLeader {
		return
	} else {
		r.becomeCandidate()
		if len(r.Prs) == 1 {
			r.becomeLeader()
			return
		}
		for k := range r.Prs {
			if k == r.id {
				continue
			}
			logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      k,
				From:    r.id,
				LogTerm: logTerm,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
			})
		}
		return
	}
}

func (r *Raft) handleMsgBeat() {
	for k := range r.Prs {
		if k == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      k,
			From:    r.id,
			Term:    r.Term,
		})
		r.logger.Infof("%x broadcast heartbeat to %x.", r.id, k)
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	for i, ent := range m.Entries {
		ent.Term = r.Term
		ent.Index = lastIndex + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
	}
	//r.Prs[r.id].Match = r.RaftLog.LastIndex()
	//r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
	for k := range r.Prs {
		if k != r.id {
			r.sendAppend(k)
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if r.Term <= m.Term {
		r.logger.Infof("%x's term %d is smaller than %x's term %d", r.id, r.Term, m.From, m.Term)
		if r.Vote == None || r.Vote == m.From {
			lastIndex := r.RaftLog.LastIndex()
			lastLogTerm, _ := r.RaftLog.Term(lastIndex)
			if lastLogTerm <= m.LogTerm {
				if lastLogTerm < m.LogTerm || lastIndex <= m.Index {
					r.Vote = m.From
					r.electionElapsed = 0
					r.msgs = append(r.msgs, pb.Message{
						MsgType: pb.MessageType_MsgRequestVoteResponse,
						To:      m.From,
						From:    m.To,
						Reject:  false,
						Term:    r.Term,
					})
					r.logger.Infof("%x send a requestVoteResponseMsg to %x", r.id, m.From)
					return
				}
			}
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    m.To,
		Reject:  true,
		Term:    r.Term,
	})
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
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    m.To,
			Index:   r.RaftLog.committed,
			Reject:  true,
		})
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.electionElapsed = 0
	rl := r.RaftLog
	if !rl.matchTerm(m.Index, m.LogTerm) {
		//Reply false if log doesn’t contain an entry at prevLogIndex
		//whose term matches prevLogTerm (§5.3)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    m.To,
			Index:   m.Index - 1, //TODO 只递减1可能效率不高
			LogTerm: m.LogTerm,
			Reject:  true,
		})
		return
	} else {
		lastnewi := m.Index + uint64(len(m.Entries))
		//find conflicts
		var ci uint64
		for _, ne := range m.Entries {
			if !rl.matchTerm(ne.Index, ne.Term) {
				//If an existing entry conflicts with a new one (same index
				//but different terms), delete the existing entry and all that
				//follow it (§5.3)
				ci := ne.Index
				rl.entries = rl.entries[:ci]
				rl.stabled = min(rl.stabled, ci-1)
				break
			}
		}
		// Append any new entries not already in the log
		offset := m.Index + 1
		for i := ci - offset; i < uint64(len(m.Entries)); i++ {
			rl.entries = append(rl.entries, *m.Entries[i])
		}
		//If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		if m.Commit > rl.committed {
			rl.committed = min(m.Commit, lastnewi)
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    m.To,
			Index:   m.Index,
		})
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if !m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
	} else {
		if m.LogTerm == None {
			r.Prs[m.From].Next = m.Index
		} else {

		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
