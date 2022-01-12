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
	key := req.GetKey()
	cf := req.GetCf()
	reader, _ := server.storage.Reader(req.GetContext())
	val, err := reader.GetCF(cf, key)
	response := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: false,
	}
	if val == nil {
		response.NotFound = true
	}
	if err != nil {
		response.Error = err.Error()
	}
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := []storage.Modify{{Data: storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}}}
	err := server.storage.Write(req.GetContext(), modify)
	response := &kvrpcpb.RawPutResponse{
		Error: "",
	}
	if err != nil {
		response.Error = err.Error()
	}
	return response, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := []storage.Modify{{Data: storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}}}
	err := server.storage.Write(nil, modify)
	response := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		response.Error = err.Error()
	}
	return response, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, _ := server.storage.Reader(req.GetContext())
	iter := reader.IterCF(req.GetCf())
	var kvPairs []*kvrpcpb.KvPair
	limit := req.GetLimit()
	var cnt uint32 = 0
	var err1 error
	iter.Seek(req.GetStartKey())
	for ; iter.Valid(); iter.Next() {
		cnt++
		if cnt > limit {
			break
		}
		item := iter.Item()
		key := item.Key()
		value, err1 := item.Value()
		if err1 != nil {
			break
		}
		kvPair := kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		}
		kvPairs = append(kvPairs, &kvPair)
	}
	response := &kvrpcpb.RawScanResponse{
		Kvs: kvPairs,
	}
	if err1 != nil {
		response.Error = err1.Error()
	}
	return response, err1
}
