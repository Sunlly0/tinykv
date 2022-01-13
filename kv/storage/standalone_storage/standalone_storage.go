package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := path.Join(conf.DBPath, "kv")
	raftPath := path.Join(conf.DBPath, "raft")

	kvEngine := engine_util.CreateDB(kvPath, false)
	raftEngine := engine_util.CreateDB(raftPath, true)

	engine := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)

	return &StandAloneStorage{engine: *engine}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	//s.engine.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneStorageReader(s.engine.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	//1.获取即将写入的数据（修改）
	for _, b := range batch {
		//2.依据类型分类处理
		switch b.Data.(type) {
		//2. Put类型
		case storage.Put:
			err := engine_util.PutCF(s.engine.Kv, b.Cf(), b.Key(), b.Value())
			if err != nil {
				return err
			}
			return nil
			//2.2 Delete类型
		case storage.Delete:
			err := engine_util.DeleteCF(s.engine.Kv, b.Cf(), b.Key())
			if err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

//实现storage Reader接口
type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		txn: txn,
	}
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	return
}
