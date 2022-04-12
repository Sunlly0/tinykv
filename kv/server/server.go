package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
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
	//1.由req.Context创建reader，进而创建mvcctxn
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.GetResponse{}, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())

	//2.读取步骤
	//2.1 检查锁，Key上是否有startTS<自己的锁，如果有，返回错误（未提交的写事务若回滚，会读到脏数据）
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return &kvrpcpb.GetResponse{}, err
	}
	if lock != nil && req.Version >= lock.Ts {
		return &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				//注：若不加PrimaryLock和LockVersion测试通不过
				Locked: &kvrpcpb.LockInfo{
					Key:         req.Key,
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
				},
			},
		}, err
	}
	//2.2 获取对应版本的值，调用GetValue（比自己startTS小的最新Write记录对应startTS的Data值)
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return &kvrpcpb.GetResponse{}, err
	}
	if value == nil {
		return &kvrpcpb.GetResponse{NotFound: true}, err
	}

	//3. 返回value
	return &kvrpcpb.GetResponse{Value: value}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	//1.由req.Context创建reader，进而创建mvcctxn
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.PrewriteResponse{}, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	//2.预写步骤，遍历Key
	var keyError []*kvrpcpb.KeyError
	for _, m := range req.Mutations {
		//2.1 检查写写冲突，是否有StartTS大于自己的Write
		//如果有，返回错误（自己若写入会覆盖startTS晚于自己的写入，不满足顺序一致性）
		write, startTs, err := txn.MostRecentWrite(m.Key)
		if err != nil {
			return &kvrpcpb.PrewriteResponse{}, err
		}
		if write != nil && req.StartVersion < startTs {
			keyError = append(keyError, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs: req.StartVersion,
				},
			})
			continue

		}
		//2.2 检查锁，如果Key上目前有锁则返回错误
		lock, err := txn.GetLock(m.Key)
		if err != nil {
			return &kvrpcpb.PrewriteResponse{}, err
		}
		if lock != nil {
			keyError = append(keyError, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					Key:         m.Key,
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
				},
			})
			continue
		}

		kind := mvcc.WriteKindFromProto(m.Op)
		//2.3 获取Key的锁，记入锁表
		txn.PutLock(m.Key, &mvcc.Lock{
			//注：缺少项测试会出错
			Primary: req.PrimaryLock,
			Kind:    kind,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
		})
		//3. 根据mutation的类型做相应操作（put/delete）
		switch m.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(m.Key)
		default:
			return nil, nil
		}
	}

	//4. 返回错误
	if len(keyError) > 0 {
		return &kvrpcpb.PrewriteResponse{
			Errors: keyError,
		}, err
	} else {
		//如果没有keyError，则将Write字段保存在数据库中
		//Q:目的？
		err = server.storage.Write(req.GetContext(), txn.Writes())
	}
	return &kvrpcpb.PrewriteResponse{}, err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	//1.由req.Context创建reader，进而创建mvcctxn
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.CommitResponse{}, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	//2.上锁，避免客户端的Commit和abort冲突
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	//3.提交阶段，遍历Key
	for _, key := range req.Keys {
		//3.1 检查Lock上的StartTs是否和本事务相同
		lock, err := txn.GetLock(key)
		if err != nil {
			return &kvrpcpb.CommitResponse{}, err
		}
		//如果没有锁，有可能是已经提交成功而客户端没有收到成功的响应,发起了重试；不作应答
		if lock == nil {
			continue
		}
		//如果Lock不是本事务的锁
		if lock.Ts != req.StartVersion {
			//该事务可能由于Prewrite阶段超时被其他事务回滚，重试
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "true",
				},
			}, err
		}
		//3.2 提交，写Write记录
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		//3.3 解锁，删Lock记录
		txn.DeleteLock(key)

	}
	//如果没有keyError，则将Write字段保存在数据库中
	err = server.storage.Write(req.GetContext(), txn.Writes())

	return &kvrpcpb.CommitResponse{}, err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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
