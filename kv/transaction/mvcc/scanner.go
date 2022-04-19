package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter   engine_util.DBIterator
	txn    *MvccTxn
	prekey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
	scanner.iter.Seek(EncodeKey(startKey, scanner.txn.StartTS))
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for ; scan.iter.Valid(); scan.iter.Next() {
		//1. 判断本事务的startTs是否可见该write
		commitTs := decodeTimestamp(scan.iter.Item().Key())
		//如果不可见，找下一个记录
		if commitTs > scan.txn.StartTS {
			continue
		}

		//可见：
		//2.判断key是否已经读取过，如果已读取，则跳过
		key := DecodeUserKey(scan.iter.Item().Key())
		if bytes.Equal(key, scan.prekey) {
			continue
		}
		scan.prekey = key

		//3.找到该key上最近的write记录，并获取startTs
		write, err := scan.iter.Item().Value()
		if err != nil {
			return nil, nil, err
		}
		parsewrite, err := ParseWrite(write)
		if err != nil {
			return nil, nil, err
		}
		ts := parsewrite.StartTS

		//4.找值,如果Write记录类型不是删除或回滚，则返回值
		var value []byte
		if parsewrite.Kind == WriteKindDelete || parsewrite.Kind == WriteKindRollback {
			//已删除，跳过
			continue
		} else {
			//正常情况，读取值
			value, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, ts))
			if err != nil {
				return nil, nil, err
			}
			//返回
			scan.iter.Next()
			return key, value, nil
		}
	}
	return nil, nil, nil
}
