package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
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

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()

		result, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			return
		}
		if result != nil {
			if !reflect.DeepEqual(result.PrevRegion, result.Region) {
				d.peerStorage.SetRegion(result.Region)
				storeMeta := d.ctx.storeMeta
				storeMeta.Lock()
				storeMeta.regions[result.Region.Id] = result.Region
				storeMeta.regionRanges.Delete(&regionItem{region: result.PrevRegion})
				storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
				storeMeta.Unlock()
			}
		}

		d.Send(d.ctx.trans, rd.Messages)

		if len(rd.CommittedEntries) > 0 {
			//oldProposals := d.proposals
			kvWB := new(engine_util.WriteBatch)
			for _, ent := range rd.CommittedEntries {
				d.processEntry(&ent, kvWB)
				d.peerStorage.applyState.AppliedIndex = ent.Index
				kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				if d.stopped {
					WB := new(engine_util.WriteBatch)
					WB.DeleteMeta(meta.ApplyStateKey(d.regionId))
					WB.MustWriteToDB(d.peerStorage.Engines.Kv)
					return
				}
				kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
				kvWB.Reset()
			}
			//d.peerStorage.applyState.AppliedIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
			//kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			//kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
			//if len(oldProposals) > len(d.proposals) {
			//	proposals := make([]*proposal, len(d.proposals))
			//	copy(proposals, d.proposals)
			//	d.proposals = proposals
			//}
		}

		d.RaftGroup.Advance(rd)
	}
	// Your Code Here (2B).
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

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	key := getReqKey(msg.Requests[0])
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	p := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	d.proposals = append(d.proposals, p)
	err = d.RaftGroup.Propose(data)
	if err != nil {
		panic(err)
	}
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	//if len(msg.Requests) != 0 {
	//	d.proposeRequest(msg, cb)
	//}
	raft.LogPrint("[region %d node %d] wants to propose RaftCmd[%v]", log.LOG_LEVEL_WARN, d.regionId, d.Meta.Id, msg)
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeRequest(msg, cb)
	}
	// Your Code Here (2B).
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
		//log.Infof("here here")
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
		log.Infof("1")
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

func (d *peerMsgHandler) handleProposals(ent *eraftpb.Entry, handle func(proposal2 *proposal)) {
	var i int
	for i = 0; i < len(d.proposals) && d.proposals[i].index < ent.Index; i++ {
		d.proposals[i].cb.Done(ErrResp(&util.ErrStaleCommand{}))
	}
	d.proposals = d.proposals[i:]
	if len(d.proposals) > 0 && d.proposals[0].index == ent.Index {
		p := d.proposals[0]
		if p.term != ent.Term {
			NotifyStaleReq(ent.Term, p.cb)
		} else {
			handle(p)
		}
		d.proposals = d.proposals[1:]
	}
}

func (d *peerMsgHandler) processEntry(ent *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	if ent.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := &eraftpb.ConfChange{}
		err := cc.Unmarshal(ent.Data)
		if err != nil {
			panic(err)
		}
		d.processEntryConfChange(ent, cc, kvWB)
		return
	}

	msg := new(raft_cmdpb.RaftCmdRequest)
	err := msg.Unmarshal(ent.Data)

	err = util.CheckRegionEpoch(msg, d.Region(), true)
	if err != nil {
		d.handleProposals(ent, func(p *proposal) {
			p.cb.Done(ErrResp(err))
		})
		//log.Infof("msg epochNotMatched.")
		return
	}
	if err != nil {
		return
	}
	if len(msg.Requests) == 0 {
		if msg.AdminRequest != nil {
			d.processEntryAdmin(ent, msg, kvWB)
		}
		return
	}
	req := msg.Requests[0]
	key := getReqKey(req)
	if key != nil {
		err = util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			d.handleProposals(ent, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			//log.Infof("msg key not in region.")
			return
		}
	}
	//kvWB := new(engine_util.WriteBatch)
	//log.Infof("Handling message[type:%v]", req.CmdType)
	raft.LogPrint("[region %d node %d] is applying CmdRequest[%v]", log.LOG_LEVEL_WARN, d.regionId, d.Meta.Id, req)
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
	case raft_cmdpb.CmdType_Snap:
	case raft_cmdpb.CmdType_Put:
		//log.Infof("Putting CF %v[key:%s, value:%s]", req.Put.Cf, req.Put.Key, req.Put.Value)
		d.peer.SizeDiffHint += uint64(len(req.Put.Key))
		d.peer.SizeDiffHint += uint64(len(req.Put.Value))
		kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
		//d.peerStorage.applyState.AppliedIndex = ent.Index
		//kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		//kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		//kvWB = new(engine_util.WriteBatch)
	case raft_cmdpb.CmdType_Delete:
		//log.Infof("Deleting CF %v[key:%s]", req.Delete.Cf, req.Delete.Key)
		d.peer.SizeDiffHint -= uint64(len(req.Delete.Key))
		kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
		//d.peerStorage.applyState.AppliedIndex = ent.Index
		//kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		//kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		//kvWB = new(engine_util.WriteBatch)
	}
	d.handleProposals(ent, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get,
				Get: &raft_cmdpb.GetResponse{Value: value}}}
		case raft_cmdpb.CmdType_Put:
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put,
				Put: &raft_cmdpb.PutResponse{}}}
		case raft_cmdpb.CmdType_Delete:
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete,
				Delete: &raft_cmdpb.DeleteResponse{}}}
		case raft_cmdpb.CmdType_Snap:
			if msg.Header.RegionEpoch.GetVersion() != d.Region().RegionEpoch.GetVersion() {
				p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
				return
			}
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
		p.cb.Done(resp)
	})
	//kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		err = d.RaftGroup.Propose(data)
		if err != nil {
			return
		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		leadTransferee := req.TransferLeader.Peer.Id
		d.RaftGroup.TransferLeader(leadTransferee)
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
			},
		})
	case raft_cmdpb.AdminCmdType_ChangePeer:
		if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
			return
		}
		if req.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode {
			raft.LogPrint("Region %d[leader: %d ] wants to remove peer %d", log.LOG_LEVEL_WARN, d.regionId, d.Meta.Id, req.ChangePeer.Peer.Id)
		} else {
			raft.LogPrint("Region %d[leader: %d ] wants to add peer %d", log.LOG_LEVEL_WARN, d.regionId, d.Meta.Id, req.ChangePeer.Peer.Id)
		}
		//if len(d.RaftGroup.Raft.Prs) == 2 && req.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode && req.ChangePeer.Peer.Id == d.Meta.Id {
		//	log.Infof("corner case!! leader node %d wants to destroy itself", d.Meta.Id)
		//	var leadTransferee uint64
		//	for peerId, _ := range d.RaftGroup.Raft.Prs {
		//		if peerId != d.Meta.Id {
		//			leadTransferee = peerId
		//			break
		//		}
		//	}
		//	d.RaftGroup.TransferLeader(leadTransferee)
		//	cb.Done(ErrResp(&util.ErrStaleCommand{}))
		//	return
		//}
		context, _ := msg.Marshal()
		confChange := eraftpb.ConfChange{
			ChangeType: req.ChangePeer.ChangeType,
			NodeId:     req.ChangePeer.Peer.Id,
			Context:    context,
		}
		p := &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}
		d.proposals = append(d.proposals, p)
		err := d.RaftGroup.ProposeConfChange(confChange)
		if err != nil {
			return
		}
	case raft_cmdpb.AdminCmdType_Split:
		split := msg.AdminRequest.Split
		err := util.CheckKeyInRegion(split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		err = util.CheckRegionEpoch(msg, d.Region(), true)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		p := &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}
		d.proposals = append(d.proposals, p)
		raft.LogPrint("region %d node(leader) %d wants to split at key %d", log.LOG_LEVEL_WARN, d.regionId, d.Meta.Id, split.SplitKey)
		err = d.RaftGroup.Propose(data)
		if err != nil {
			return
		}
	}
}

func (d *peerMsgHandler) processEntryAdmin(ent *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactLog := req.GetCompactLog()
		if compactLog.CompactIndex >= d.peerStorage.truncatedIndex() {
			d.peerStorage.applyState.TruncatedState.Index = compactLog.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			//kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
			d.ScheduleCompactLog(d.peerStorage.applyState.TruncatedState.Index)
		}
	case raft_cmdpb.AdminCmdType_Split:
		split := req.Split
		err := util.CheckKeyInRegion(split.SplitKey, d.Region())
		if err != nil {
			d.handleProposals(ent, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return
		}
		err = util.CheckRegionEpoch(msg, d.Region(), true)
		if err != nil {
			d.handleProposals(ent, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return
		}
		if len(d.Region().Peers) != len(split.NewPeerIds) {
			d.handleProposals(ent, func(p *proposal) {
				p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			})
			return
		}
		//raft.LogPrint("%d is spliting at key %d", log.LOG_LEVEL_WARN, d.regionId, d.Meta.Id, split.SplitKey)
		raft.LogPrint("[region %d node %d] is applying region split[%v]", log.LOG_LEVEL_WARN, d.regionId, d.Meta.Id, split)
		leftRegion := d.Region()
		rightRegion := &metapb.Region{}
		err = util.CloneMsg(leftRegion, rightRegion)
		if err != nil {
			d.handleProposals(ent, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return
		}
		leftRegion.RegionEpoch.Version++
		rightRegion.RegionEpoch.Version++
		rightRegion.Id = split.NewRegionId
		rightRegion.StartKey = split.SplitKey
		rightRegion.EndKey = leftRegion.EndKey
		newPeers := make([]*metapb.Peer, 0)
		for i, peer := range leftRegion.Peers {
			newPeers = append(newPeers, &metapb.Peer{
				Id:      split.NewPeerIds[i],
				StoreId: peer.StoreId,
			})
		}
		rightRegion.Peers = newPeers
		leftRegion.EndKey = split.SplitKey
		meta.WriteRegionState(kvWB, leftRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, rightRegion, rspb.PeerState_Normal)
		//kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		//kvWB = new(engine_util.WriteBatch)

		peer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, rightRegion)
		if err != nil {
			d.handleProposals(ent, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return
		}
		d.ctx.router.register(peer)
		err = d.ctx.router.send(rightRegion.Id, message.Msg{
			Type:     message.MsgTypeStart,
			RegionID: rightRegion.Id,
		})
		if err != nil {
			d.handleProposals(ent, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return
		}

		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regionRanges.Delete(&regionItem{leftRegion})
		storeMeta.regions[rightRegion.Id] = rightRegion // 用setRegion就不行
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{leftRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{rightRegion})
		storeMeta.Unlock()
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)

		d.handleProposals(ent, func(p *proposal) {
			p.cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_Split,
					Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{leftRegion, rightRegion}},
				},
			})
		})
		//if d.IsLeader() {
		//	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		//}
		d.notifyHeartbeatScheduler(leftRegion, d.peer)
		d.notifyHeartbeatScheduler(rightRegion, peer)
	}
}

func (d *peerMsgHandler) processEntryConfChange(ent *eraftpb.Entry, cc *eraftpb.ConfChange, kvWB *engine_util.WriteBatch) {
	region := d.Region()
	req := &raft_cmdpb.RaftCmdRequest{}
	err := req.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	err = util.CheckRegionEpoch(req, region, true)
	if errEpochNotMatch, ok := err.(*util.ErrEpochNotMatch); ok {
		d.handleProposals(ent, func(p *proposal) {
			p.cb.Done(ErrResp(errEpochNotMatch))
		})
		return
	}
	raft.LogPrint("[region %d node %d] is applying conf change[%v]", log.LOG_LEVEL_WARN, d.regionId, d.Meta.Id, cc)
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		raft.LogPrint("add new node %d", log.LOG_LEVEL_WARN, cc.NodeId)
		ind := isPeerExist(region, cc.NodeId)
		if ind == len(region.Peers) {
			//the peer doesn't exist
			log.Infof("%d is adding node %d", d.PeerId(), cc.NodeId)
			newPeer := req.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers, newPeer)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.insertPeerCache(newPeer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if cc.NodeId == d.Meta.Id {
			log.Infof("node %d is destroying itself", cc.NodeId)
			d.sendMoreHeartBeat()
			d.destroyPeer()
			return
		}
		ind := isPeerExist(region, cc.NodeId)
		if ind < len(region.Peers) {
			//the peer exists
			region.Peers = append(region.Peers[:ind], region.Peers[ind+1:]...)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.removePeerCache(cc.NodeId)
		}
	}
	d.RaftGroup.ApplyConfChange(*cc)
	//wb.MustWriteToDB(d.peerStorage.Engines.Kv)
	d.handleProposals(ent, func(p *proposal) {
		p.cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{},
			},
		})
	})
	//if d.IsLeader() {
	//	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	//}
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
}

func getReqKey(req *raft_cmdpb.Request) []byte {
	var key []byte
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	}
	return key
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) sendMoreHeartBeat() {
	if len(d.Region().Peers) == 2 && d.IsLeader() {
		raft.LogPrint("Encounter corner case now!!", log.LOG_LEVEL_WARN)
		var targetPeer uint64 = 0
		for _, peer := range d.Region().Peers {
			if peer.Id != d.Meta.Id {
				targetPeer = peer.Id
				break
			}
		}
		m := []eraftpb.Message{{
			MsgType: eraftpb.MessageType_MsgHeartbeat,
			To:      targetPeer,
			Commit:  d.peerStorage.raftState.HardState.Commit,
		}}
		for i := 0; i < 10; i++ {
			d.Send(d.ctx.trans, m)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func isPeerExist(region *metapb.Region, id uint64) int {
	for i, peer := range region.Peers {
		if peer.Id == id {
			return i
		}
	}
	return len(region.Peers)
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
