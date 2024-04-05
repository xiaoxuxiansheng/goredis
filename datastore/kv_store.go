package datastore

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xiaoxuxiansheng/goredis/database"
	"github.com/xiaoxuxiansheng/goredis/handler"
	"github.com/xiaoxuxiansheng/goredis/lib"
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

// expire
func (k *KVStore) Expire(args [][]byte) handler.Reply {
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}
	if ttl <= 0 {
		return handler.NewErrReply("ERR invalid expire time")
	}
	k.expire(key, lib.TimeNow().Add(time.Duration(ttl)*time.Second))
	return handler.NewOKReply()
}

// string
func (k *KVStore) Get(args [][]byte) handler.Reply {
	key := string(args[0])
	v, err := k.getAsString(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}
	return handler.NewBulkReply(v.Bytes())
}

func (k *KVStore) MGet(args [][]byte) handler.Reply {
	res := make([][]byte, len(args))
	for _, arg := range args {
		v, err := k.getAsString(string(arg))
		if err != nil {
			return handler.NewErrReply(err.Error())
		}
		res = append(res, v.Bytes())
	}

	return handler.NewMultiBulkReply(res)
}

func (k *KVStore) Set(args [][]byte) handler.Reply {
	key := string(args[0])
	value := string(args[1])

	// 支持 NX EX
	var (
		insertStrategy bool
		ttlStrategy    bool
		ttlSeconds     int64
	)

	for i := 2; i < len(args); i++ {
		flag := strings.ToLower(string(args[i]))
		switch flag {
		case "nx":
			insertStrategy = true
		case "ex":
			if i == len(args)-1 {
				return handler.NewSyntaxErrReply()
			}
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return handler.NewSyntaxErrReply()
			}
			if ttl <= 0 {
				return handler.NewErrReply("ERR invalid expire time")
			}

			ttlStrategy = true
			ttlSeconds = ttl
			i++
		default:
			return handler.NewSyntaxErrReply()
		}
	}

	// 设置
	affected := k.put(key, value, insertStrategy)
	if affected > 0 && ttlStrategy {
		k.expire(key, lib.TimeNow().Add(time.Duration(ttlSeconds)*time.Second))
	}

	// 过期时间处理
	if affected > 0 {
		return handler.NewIntReply(affected)
	}

	return handler.NewNillReply()
}

func (k *KVStore) MSet(args [][]byte) handler.Reply {
	if len(args)&1 == 1 {
		return handler.NewSyntaxErrReply()
	}

	for i := 0; i < len(args); i += 2 {
		_ = k.put(string(args[i]), string(args[i+1]), false)
	}

	return handler.NewIntReply(int64(len(args) >> 1))
}

// list
func (k *KVStore) LPush(args [][]byte) handler.Reply {
	key := string(args[0])
	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		list = newListEntity()
		k.putAsList(key, list)
	}

	for i := 1; i < len(args); i++ {
		list.LPush(args[i])
	}

	return handler.NewIntReply(list.Len())
}

func (k *KVStore) LPop(args [][]byte) handler.Reply {
	key := string(args[0])
	var cnt int64
	if len(args) > 1 {
		rawCnt, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return handler.NewSyntaxErrReply()
		}
		if rawCnt < 1 {
			return handler.NewSyntaxErrReply()
		}
		cnt = rawCnt
	}

	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		return handler.NewNillReply()
	}

	if cnt == 0 {
		cnt = 1
	}

	poped := list.LPop(cnt)
	if poped == nil {
		return handler.NewNillReply()
	}

	if len(poped) == 1 {
		return handler.NewBulkReply(poped[0])
	}

	return handler.NewMultiBulkReply(poped)
}

func (k *KVStore) RPush(args [][]byte) handler.Reply {
	key := string(args[0])
	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		list = newListEntity(args[1:]...)
		k.putAsList(key, list)
		return handler.NewIntReply(list.Len())
	}

	for i := 1; i < len(args); i++ {
		list.RPush(args[i])
	}

	return handler.NewIntReply(list.Len())
}

func (k *KVStore) RPop(args [][]byte) handler.Reply {
	key := string(args[0])
	var cnt int64
	if len(args) > 1 {
		rawCnt, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return handler.NewSyntaxErrReply()
		}
		if rawCnt < 1 {
			return handler.NewSyntaxErrReply()
		}
		cnt = rawCnt
	}

	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		return handler.NewNillReply()
	}

	if cnt == 0 {
		cnt = 1
	}

	poped := list.RPop(cnt)
	if poped == nil {
		return handler.NewNillReply()
	}

	if len(poped) == 1 {
		return handler.NewBulkReply(poped[0])
	}

	return handler.NewMultiBulkReply(poped)
}

func (k *KVStore) LRange(args [][]byte) handler.Reply {
	if len(args) != 3 {
		return handler.NewSyntaxErrReply()
	}

	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}

	stop, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}

	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		return handler.NewNillReply()
	}

	if got := list.Range(start, stop); got != nil {
		return handler.NewMultiBulkReply(got)
	}

	return handler.NewNillReply()
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
