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
	//2.2 获取对应版本的值，调用GetValue（比自己startTS小的最新Write记录对应的Data值)
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

	// //2.上锁，避免客户端的Commit和abort冲突
	// server.Latches.WaitForLatches(req.Keys)
	// defer server.Latches.ReleaseLatches(req.Keys)

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
	//1.由req.Context创建reader，进而创建mvcctxn
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.ScanResponse{}, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())

	//2.创建scanner
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	//3.scanner调用Next()，取limit个数的值，注意事务的处理
	var kvs []*kvrpcpb.KvPair
	limit := req.GetLimit()
	for limit > 0 {
		//2.1 scanner.Next()，迭代取pair对
		key, value, err := scanner.Next()
		if err != nil {
			return &kvrpcpb.ScanResponse{}, err
		}
		//2.2 检查key是否存在
		if key == nil {
			break
		}
		//2.3 检查数据key上是否有锁
		lock, err := txn.GetLock(key)
		if err != nil {
			return &kvrpcpb.ScanResponse{}, err
		}
		//如果有锁，且锁事务的startTs<本事务
		if lock != nil && txn.StartTS >= lock.Ts {
			kvs = append(kvs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			})
			limit--
			continue
		}
		//2.4 正常情况，创建pair对并append
		pair := &kvrpcpb.KvPair{Key: key, Value: value}
		kvs = append(kvs, pair)
		limit--
	}

	if len(kvs) == 0 {
		return &kvrpcpb.ScanResponse{}, err
	}
	return &kvrpcpb.ScanResponse{Pairs: kvs}, err
}

//检查事务的状态：锁未超时；锁超时；
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	//1.由req.Context创建reader，进而创建mvcctxn
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.CheckTxnStatusResponse{}, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())

	//2.按是否有锁分情况讨论
	lock, err := txn.GetLock(req.PrimaryKey)
	//2.1 如果还有锁
	if lock != nil {
		//检查锁是否超时
		//2.1.1 如果锁超时
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
			//abort事务：解锁，删除数据，将Write记录置为WriteKindRollback
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			err = server.storage.Write(req.Context, txn.Writes())
			if err == nil {
				return &kvrpcpb.CheckTxnStatusResponse{
					LockTtl: 0,
					Action:  kvrpcpb.Action_TTLExpireRollback,
				}, nil
			}
		} else {
			//2.1.2 锁没有超时
			return &kvrpcpb.CheckTxnStatusResponse{
				LockTtl: lock.Ttl,
			}, nil
		}
	} else {
		//2.2 没有lock，检查事务的提交记录(以主键为准)
		write, ts, err := txn.CurrentWrite(req.PrimaryKey)
		if write != nil {
			//2.2.1 已经提交且不是WriteKindRollback类型
			if write.Kind != mvcc.WriteKindRollback {
				return &kvrpcpb.CheckTxnStatusResponse{
					CommitVersion: ts,
				}, err
			}
		}
		//2.2.2 WriteKindRollback和其他情况
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err == nil {
			return &kvrpcpb.CheckTxnStatusResponse{
				Action: kvrpcpb.Action_LockNotExistRollback,
			}, nil
		}
	}
	return &kvrpcpb.CheckTxnStatusResponse{}, err
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	//1.由req.Context创建reader，进而创建mvcctxn
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{}, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	//2.上锁，避免客户端的Commit和abort冲突
	// server.Latches.WaitForLatches(req.Keys)
	// defer server.Latches.ReleaseLatches(req.Keys)

	//3.遍历key，并依次回滚
	for _, key := range req.Keys {
		//3.0 对当前事务对应的Write检查
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return &kvrpcpb.BatchRollbackResponse{}, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				//已经回滚,跳过该key（可能是其他事务回滚的）
				continue
			}
			return &kvrpcpb.BatchRollbackResponse{Error: &kvrpcpb.KeyError{Abort: "true"}}, nil
		}

		//3.1 检查Lock是否存在，以及是否还是本事务持有
		lock, err := txn.GetLock(key)
		if err != nil {
			return &kvrpcpb.BatchRollbackResponse{}, err
		}
		//3.2 修改Write的kind为回滚(Write存在则修改Kind；Write不存在则添加记录)
		txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
		//如果没有锁（已释放），或不是本事务持有了，则终止回滚
		if lock == nil || lock.Ts != req.GetStartVersion() {
			continue
		}

		//3.3 本事务存在锁的情况下，删除锁和数据
		txn.DeleteLock(key)
		txn.DeleteValue(key)
	}

	//4. 如果没有keyError，则将Write字段保存在数据库中
	err = server.storage.Write(req.GetContext(), txn.Writes())

	return &kvrpcpb.BatchRollbackResponse{}, err
}

//找属于这个事务的所有锁，同时回滚或提交
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	//1.由req.Context创建reader，进而创建mvcctxn
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{}, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	//2.获取本事务所有锁定的keys
	keys, err := txn.GetKeysInLock()
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{}, err
	}
	//如果没有key锁定，不作操作
	if len(keys) == 0 {
		return &kvrpcpb.ResolveLockResponse{}, nil
	}

	//3.回滚或提交，取决于CommitVersion(Q:为什么不是取决PrimaryKey?）
	if req.CommitVersion == 0 {
		//回滚
		resp, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		return &kvrpcpb.ResolveLockResponse{
			RegionError: resp.RegionError,
			Error:       resp.Error,
		}, err
	} else {
		//提交
		resp, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		return &kvrpcpb.ResolveLockResponse{
			RegionError: resp.RegionError,
			Error:       resp.Error,
		}, err
	}
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
