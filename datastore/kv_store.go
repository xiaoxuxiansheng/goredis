package datastore

import (
	"strconv"
	"strings"
	"time"

	"github.com/xiaoxuxiansheng/goredis/database"
	"github.com/xiaoxuxiansheng/goredis/handler"
	"github.com/xiaoxuxiansheng/goredis/lib"
)

type KVStore struct {
	data      map[string]interface{}
	expiredAt map[string]time.Time

	expireTimeWheel SortedSet

	persister Persister
}

func NewKVStore(persister Persister) database.DataStore {
	if kvStore := persister.Reload(); kvStore != nil {
		return kvStore
	}

	return &KVStore{
		data:            make(map[string]interface{}),
		expiredAt:       make(map[string]time.Time),
		expireTimeWheel: newSkiplist(),
		persister:       persister,
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

	expireAt := lib.TimeNow().Add(time.Duration(ttl) * time.Second)
	_args := [][]byte{[]byte(database.CmdTypeExpireAt), []byte(key), []byte(lib.TimeSecondFormat(expireAt))}
	return k.expireAt(_args, key, expireAt)
}

func (k *KVStore) ExpireAt(args [][]byte) handler.Reply {
	key := string(args[0])
	expiredAt, err := lib.ParseTimeSecondFormat(string((args[1])))
	if err != nil {
		return handler.NewSyntaxErrReply()
	}
	if expiredAt.Before(lib.TimeNow()) {
		return handler.NewErrReply("ERR invalid expire time")
	}

	return k.expireAt(args, key, expiredAt)
}

func (k *KVStore) expireAt(args [][]byte, key string, expireAt time.Time) handler.Reply {
	k.expire(key, expireAt)
	k.persister.PersistCmd(args) // 持久化
	return handler.NewOKReply()
}

// string
func (k *KVStore) Get(args [][]byte) handler.Reply {
	key := string(args[0])
	v, err := k.getAsString(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}
	if v == nil {
		return handler.NewNillReply()
	}
	return handler.NewBulkReply(v.Bytes())
}

func (k *KVStore) MGet(args [][]byte) handler.Reply {
	res := make([][]byte, 0, len(args))
	for _, arg := range args {
		v, err := k.getAsString(string(arg))
		if err != nil {
			return handler.NewErrReply(err.Error())
		}
		if v == nil {
			res = append(res, []byte("(nil)"))
			continue
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
			// 重复的 ex 指令
			if ttlStrategy {
				return handler.NewSyntaxErrReply()
			}
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
			// 将 args 剔除 ex 部分，进行持久化
			args = append(args[:i], args[i+2:]...)
			i++
		default:
			return handler.NewSyntaxErrReply()
		}
	}

	// 设置
	affected := k.put(key, value, insertStrategy)
	if ttlStrategy {
		expireAt := lib.TimeNow().Add(time.Duration(ttlSeconds) * time.Second)
		_args := [][]byte{[]byte(database.CmdTypeExpireAt), []byte(key), []byte(lib.TimeSecondFormat(expireAt))}
		_ = k.expireAt(_args, key, expireAt) // 其中会完成 ex 信息的持久化
	}

	// 过期时间处理
	if affected > 0 {
		k.persister.PersistCmd(args)
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

	k.persister.PersistCmd(args)
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

	k.persister.PersistCmd(args)
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

	k.persister.PersistCmd(args) // 持久化

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

	k.persister.PersistCmd(args) // 持久化
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

	k.persister.PersistCmd(args) // 持久化
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

	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
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
	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if set == nil {
		set = newSetEntity()
		k.putAsSet(key, set)
	}

	var added int64
	for _, arg := range args[1:] {
		added += set.Add(string(arg))
	}

	k.persister.PersistCmd(args) // 持久化
	return handler.NewIntReply(added)
}

func (k *KVStore) SIsMember(args [][]byte) handler.Reply {
	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if set == nil {
		return handler.NewIntReply(0)
	}

	return handler.NewIntReply(set.Exist(string(args[1])))
}

func (k *KVStore) SRem(args [][]byte) handler.Reply {
	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if set == nil {
		return handler.NewIntReply(0)
	}

	var remed int64
	for _, arg := range args[1:] {
		remed += set.Rem(string(arg))
	}

	if remed > 0 {
		k.persister.PersistCmd(args) // 持久化
	}
	return handler.NewIntReply(remed)
}

// hash
func (k *KVStore) HSet(args [][]byte) handler.Reply {
	if len(args)&1 != 1 {
		return handler.NewSyntaxErrReply()
	}

	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if hmap == nil {
		hmap = newHashMapEntity()
		k.putAsHashMap(key, hmap)
	}

	for i := 0; i < len(args)-1; i += 2 {
		hkey := string(args[i+1])
		hvalue := args[i+2]
		hmap.Put(hkey, hvalue)
	}

	k.persister.PersistCmd(args) // 持久化
	return handler.NewIntReply(int64((len(args) - 1) >> 1))
}

func (k *KVStore) HGet(args [][]byte) handler.Reply {
	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if hmap == nil {
		return handler.NewNillReply()
	}

	if v := hmap.Get(string(args[1])); v != nil {
		return handler.NewBulkReply(v)
	}

	return handler.NewNillReply()
}

func (k *KVStore) HDel(args [][]byte) handler.Reply {
	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if hmap == nil {
		return handler.NewIntReply(0)
	}

	var remed int64
	for _, arg := range args[1:] {
		remed += hmap.Del(string(arg))
	}

	if remed > 0 {
		k.persister.PersistCmd(args) // 持久化
	}
	return handler.NewIntReply(remed)
}

// sorted set
func (k *KVStore) ZAdd(args [][]byte) handler.Reply {
	if len(args)&1 != 1 {
		return handler.NewSyntaxErrReply()
	}

	key := string(args[0])
	var (
		scores  = make([]int64, 0, (len(args)-1)>>1)
		members = make([]string, 0, (len(args)-1)>>1)
	)

	for i := 0; i < len(args)-1; i += 2 {
		score, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil {
			return handler.NewSyntaxErrReply()
		}

		scores = append(scores, score)
		members = append(members, string(args[i+2]))
	}

	zset, err := k.getAsSortedSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if zset == nil {
		zset = newSkiplist()
		k.putAsSortedSet(key, zset)
	}

	for i := 0; i < len(scores); i++ {
		zset.Add(scores[i], members[i])
	}

	k.persister.PersistCmd(args) // 持久化
	return handler.NewIntReply(int64(len(scores)))
}

func (k *KVStore) ZRangeByScore(args [][]byte) handler.Reply {
	if len(args) < 3 {
		return handler.NewSyntaxErrReply()
	}

	key := string(args[0])
	score1, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}
	score2, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}

	zset, err := k.getAsSortedSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if zset == nil {
		return handler.NewNillReply()
	}

	rawRes := zset.Range(score1, score2)
	if len(rawRes) == 0 {
		return handler.NewNillReply()
	}

	res := make([][]byte, 0, len(rawRes))
	for _, item := range rawRes {
		res = append(res, []byte(item))
	}

	return handler.NewMultiBulkReply(res)
}

func (k *KVStore) ZRem(args [][]byte) handler.Reply {
	key := string(args[0])
	zset, err := k.getAsSortedSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if zset == nil {
		return handler.NewIntReply(0)
	}

	var remed int64
	for _, arg := range args {
		remed += zset.Rem(string(arg))
	}

	if remed > 0 {
		k.persister.PersistCmd(args) // 持久化
	}
	return handler.NewIntReply(remed)
}
