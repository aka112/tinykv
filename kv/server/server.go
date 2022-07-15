package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	keyReq := req.Key
	server.Latches.AcquireLatches([][]byte{keyReq})
	defer server.Latches.ReleaseLatches([][]byte{keyReq})
	memReader, _ := server.storage.Reader(req.Context)
	defer memReader.Close()
	mvccTxn := mvcc.NewMvccTxn(memReader, req.Version)
	lockGet, _ := mvccTxn.GetLock(keyReq)
	if lockGet != nil && lockGet.Ts < req.Version {
		resp := &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{Locked: lockGet.Info(keyReq)},
		}
		return resp, nil
	}
	value, _ := mvccTxn.GetValue(keyReq)
	if value == nil {
		return &kvrpcpb.GetResponse{
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.GetResponse{
		Value: value,
	}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	mutations := req.Mutations
	keys := make([][]byte, 0)
	for _, mutation := range mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	memReader, _ := server.storage.Reader(req.Context)
	defer memReader.Close()
	mvccTxn := mvcc.NewMvccTxn(memReader, req.StartVersion)
	resp := &kvrpcpb.PrewriteResponse{}
	for _, key := range keys {
		_, writeCommitTs, _ := mvccTxn.MostRecentWrite(key)
		if writeCommitTs > req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: writeCommitTs,
					Key:        key,
				},
			})
		}
		if lock, _ := mvccTxn.GetLock(key); lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(key),
			})
		}
	}
	if resp.Errors != nil {
		return resp, nil
	}
	for _, mutation := range mutations {
		mvccTxn.PutValue(mutation.Key, mutation.Value)
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		}
		mvccTxn.PutLock(mutation.Key, lock) // key最开始搞错了 赋成了req.PrimaryLock
	}
	writes := mvccTxn.Writes()
	//log.Infof("num %d %v", len(writes), writes[1])
	err := server.storage.Write(req.Context, writes)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	keys := req.Keys
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	memReader, _ := server.storage.Reader(req.Context)
	defer memReader.Close()
	mvccTxn := mvcc.NewMvccTxn(memReader, req.StartVersion)
	resp := &kvrpcpb.CommitResponse{}
	for _, key := range keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "conflict"}
			return resp, nil
		}
	}
	for _, key := range keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			write := &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    lock.Kind,
			}
			mvccTxn.PutWrite(key, req.CommitVersion, write)
			mvccTxn.DeleteLock(key)
		}
	}
	writes := mvccTxn.Writes()
	err := server.storage.Write(req.Context, writes)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	memReader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(memReader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer memReader.Close()
	defer scanner.Close()
	resp := &kvrpcpb.ScanResponse{}
	var cnt uint32
	for cnt < req.Limit {
		//log.Infof("cnt %d", cnt)
		key, value, _ := scanner.Next()
		if key == nil {
			break
		}
		lockGet, _ := mvccTxn.GetLock(key)
		if lockGet != nil && lockGet.Ts < req.Version {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: lockGet.Info(key),
				},
			})
		}
		if value != nil {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			})
		}
		cnt++
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	memReader, _ := server.storage.Reader(req.Context)
	defer memReader.Close()
	mvccTxn := mvcc.NewMvccTxn(memReader, req.LockTs)
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	write, writeTs, _ := mvccTxn.CurrentWrite(req.PrimaryKey)
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = writeTs
		}
		return resp, nil
	}
	lock, _ := mvccTxn.GetLock(req.PrimaryKey)
	if lock == nil {
		write := &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: req.LockTs,
		}
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, write)
		writes := mvccTxn.Writes()
		err := server.storage.Write(req.Context, writes)
		if err != nil {
			return nil, err
		}
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}
	//log.Infof("%v %v %v", mvcc.PhysicalTime(req.LockTs), lock.Ttl, mvcc.PhysicalTime(req.CurrentTs))
	if mvcc.PhysicalTime(req.LockTs)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		mvccTxn.DeleteValue(req.PrimaryKey)
		mvccTxn.DeleteLock(req.PrimaryKey)
		write := &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: req.LockTs,
		}
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, write)
		writes := mvccTxn.Writes()
		err := server.storage.Write(req.Context, writes)
		if err != nil {
			return nil, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		return resp, nil
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	memReader, _ := server.storage.Reader(req.Context)
	defer memReader.Close()
	mvccTxn := mvcc.NewMvccTxn(memReader, req.StartVersion)
	resp := &kvrpcpb.BatchRollbackResponse{}
	for _, key := range req.Keys {
		lock, _ := mvccTxn.GetLock(key)
		if lock != nil && lock.Ts == req.StartVersion {
			mvccTxn.DeleteValue(key)
			mvccTxn.DeleteLock(key)
			write := &mvcc.Write{
				Kind:    mvcc.WriteKindRollback,
				StartTS: req.StartVersion,
			}
			mvccTxn.PutWrite(key, req.StartVersion, write)
		} else {
			write, _, _ := mvccTxn.CurrentWrite(key)
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					continue
				} else {
					resp.Error = &kvrpcpb.KeyError{Abort: "conflict-abort"}
					return resp, nil
				}
			} else {
				write := &mvcc.Write{
					Kind:    mvcc.WriteKindRollback,
					StartTS: req.StartVersion,
				}
				mvccTxn.PutWrite(key, req.StartVersion, write)
			}
		}
		writes := mvccTxn.Writes()
		err := server.storage.Write(req.Context, writes)
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	memReader, _ := server.storage.Reader(req.Context)
	defer memReader.Close()
	resp := &kvrpcpb.ResolveLockResponse{}
	mvccTxn := mvcc.NewMvccTxn(memReader, req.StartVersion)
	keys := make([][]byte, 0)
	iterLock := memReader.IterCF(engine_util.CfLock)
	defer iterLock.Close()
	for ; iterLock.Valid(); iterLock.Next() {
		lock, _ := mvccTxn.GetLock(iterLock.Item().Key())
		if lock != nil && lock.Ts == req.StartVersion {
			keys = append(keys, iterLock.Item().Key())
		}
	}
	if req.CommitVersion == 0 {
		rollback, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return nil, err
		}
		resp.Error = rollback.Error
		resp.RegionError = rollback.RegionError
	} else {
		commit, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return nil, err
		}
		resp.Error = commit.Error
		resp.RegionError = commit.RegionError
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
