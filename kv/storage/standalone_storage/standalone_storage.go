package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	enginesInner *engine_util.Engines
	configInner  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	enginesInner := engine_util.NewEngines(nil, nil, "", "")
	enginesInner.KvPath = conf.DBPath + "/kv"
	enginesInner.Kv = engine_util.CreateDB(enginesInner.KvPath, false)
	if conf.Raft == true {
		enginesInner.RaftPath = conf.DBPath + "/raft"
		enginesInner.Raft = engine_util.CreateDB(enginesInner.RaftPath, true)
	}
	return &StandAloneStorage{
		enginesInner: enginesInner,
		configInner:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.enginesInner.Close()
}

func (s *StandAloneStorage) Reader(_ *kvrpcpb.Context) (storage.StorageReader, error) {
	// get and scan
	txn := s.enginesInner.Kv.NewTransaction(false)
	return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(_ *kvrpcpb.Context, batch []storage.Modify) error {
	// put and delete
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.enginesInner.Kv, m.Cf(), m.Key(), m.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.enginesInner.Kv, m.Cf(), m.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (sasReader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCFFromTxn(sasReader.txn, cf, key)
	return val, nil
}

func (sasReader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sasReader.txn)
}

func (sasReader *StandAloneStorageReader) Close() {
	sasReader.txn.Discard()
}
