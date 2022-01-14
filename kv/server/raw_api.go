package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	//1。获取reader
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}
	defer reader.Close()
	//2. 获取val
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val, NotFound: false}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	//1. 调用storage.put函数
	put := storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}
	batch := storage.Modify{Data: put}
	//2、调用storage.Write函数
	err := server.storage.Write(req.GetContext(), []storage.Modify{batch})
	//错误处理
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	//1. 调用storage.delete函数
	del := storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}
	batch := storage.Modify{Data: del}
	//2、调用storage.Write函数
	err := server.storage.Write(req.GetContext(), []storage.Modify{batch})
	//错误处理
	if err != nil {
		//return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
		return &kvrpcpb.RawDeleteResponse{}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	//1. 获取reader
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}
	//2. 调用reader.IterCF函数,获取iter
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	//A:defer的作用：go语言用法，延迟调用，在return时执行，一般用于资源释放

	var kvs []*kvrpcpb.KvPair
	limit := req.GetLimit()

	//for iter.Seek(req.GetStartKey()); limit != 0; limit-- {
	//	item := iter.Item()
	//	key := item.Key()
	//	val, _ := item.Value()
	//
	//	pair := &kvrpcpb.KvPair{Key: key, Value: val}
	//	kvs = append(kvs, pair)
	//	iter.Next()
	//}
	for iter.Seek(req.GetStartKey()); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		val, _ := item.Value()

		pair := &kvrpcpb.KvPair{Key: key, Value: val}

		kvs = append(kvs, pair)
		limit--
		if limit == 0 {
			break
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil

}
