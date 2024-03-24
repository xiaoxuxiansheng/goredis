package datastore

import (
	"sync"
	"time"

	"github.com/xiaoxuxiansheng/goredis/database"
	"github.com/xiaoxuxiansheng/goredis/handler"
)

type KVStore struct {
	mu        sync.Mutex // 操作数据是单线程模型. 这把锁是为了和 expire 回收线程隔离
	data      map[string]interface{}
	expiredAt map[string]time.Time
}

func NewKVStore() database.DataStore {
	return &KVStore{
		data:      make(map[string]interface{}),
		expiredAt: make(map[string]time.Time),
	}
}

// string
func (k *KVStore) Get(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) MGet(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) Set(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) MSet(args [][]byte) handler.Reply {
	return nil
}

// list
func (k *KVStore) LPush(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) LPop(args [][]byte) handler.Reply {
	return nil
}

// set
func (k *KVStore) SAdd(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) SIsMember(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) SRem(args [][]byte) handler.Reply {
	return nil
}

// hash
func (k *KVStore) HSet(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) HGet(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) HDel(args [][]byte) handler.Reply {
	return nil
}

// sorted set
func (k *KVStore) ZAdd(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) ZRange(args [][]byte) handler.Reply {
	return nil
}

func (k *KVStore) ZRem(args [][]byte) handler.Reply {
	return nil
}
