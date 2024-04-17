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

	persister handler.Persister
}

func NewKVStore(persister handler.Persister) database.DataStore {
	return &KVStore{
		data:            make(map[string]interface{}),
		expiredAt:       make(map[string]time.Time),
		expireTimeWheel: newSkiplist("expireTimeWheel"),
		persister:       persister,
	}
}

// expire
func (k *KVStore) Expire(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}
	if ttl <= 0 {
		return handler.NewErrReply("ERR invalid expire time")
	}

	expireAt := lib.TimeNow().Add(time.Duration(ttl) * time.Second)
	_cmd := [][]byte{[]byte(database.CmdTypeExpireAt), []byte(key), []byte(lib.TimeSecondFormat(expireAt))}
	return k.expireAt(_cmd, key, expireAt)
}

func (k *KVStore) ExpireAt(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	key := string(args[0])
	expiredAt, err := lib.ParseTimeSecondFormat(string((args[1])))
	if err != nil {
		return handler.NewSyntaxErrReply()
	}
	if expiredAt.Before(lib.TimeNow()) {
		return handler.NewErrReply("ERR invalid expire time")
	}

	return k.expireAt(cmd.Cmd(), key, expiredAt)
}

func (k *KVStore) expireAt(cmd [][]byte, key string, expireAt time.Time) handler.Reply {
	k.expire(key, expireAt)
	k.persister.PersistCmd(cmd) // 持久化
	return handler.NewOKReply()
}

// string
func (k *KVStore) Get(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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

func (k *KVStore) MGet(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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

func (k *KVStore) Set(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	key := string(args[0])
	value := string(args[1])

	// 支持 NX EX
	var (
		insertStrategy bool
		ttlStrategy    bool
		ttlSeconds     int64
		ttlIndex       = -1
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
			ttlIndex = i
			i++
		default:
			return handler.NewSyntaxErrReply()
		}
	}

	// 将 args 剔除 ex 部分，进行持久化
	if ttlIndex != -1 {
		args = append(args[:ttlIndex], args[ttlIndex+2:]...)
	}

	// 设置
	affected := k.put(key, value, insertStrategy)
	if affected > 0 && ttlStrategy {
		expireAt := lib.TimeNow().Add(time.Duration(ttlSeconds) * time.Second)
		_cmd := [][]byte{[]byte(database.CmdTypeExpireAt), []byte(key), []byte(lib.TimeSecondFormat(expireAt))}
		_ = k.expireAt(_cmd, key, expireAt) // 其中会完成 ex 信息的持久化
	}

	// 过期时间处理
	if affected > 0 {
		k.persister.PersistCmd(append([][]byte{[]byte(database.CmdTypeSet)}, args...))
		return handler.NewIntReply(affected)
	}

	return handler.NewNillReply()
}

func (k *KVStore) MSet(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	if len(args)&1 == 1 {
		return handler.NewSyntaxErrReply()
	}

	for i := 0; i < len(args); i += 2 {
		_ = k.put(string(args[i]), string(args[i+1]), false)
	}

	k.persister.PersistCmd(cmd.Cmd())
	return handler.NewIntReply(int64(len(args) >> 1))
}

// list
func (k *KVStore) LPush(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	key := string(args[0])
	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		list = newListEntity(key)
		k.putAsList(key, list)
	}

	for i := 1; i < len(args); i++ {
		list.LPush(args[i])
	}

	k.persister.PersistCmd(cmd.Cmd())
	return handler.NewIntReply(list.Len())
}

func (k *KVStore) LPop(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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

	k.persister.PersistCmd(cmd.Cmd()) // 持久化

	if len(poped) == 1 {
		return handler.NewBulkReply(poped[0])
	}

	return handler.NewMultiBulkReply(poped)
}

func (k *KVStore) RPush(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	key := string(args[0])
	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		list = newListEntity(key, args[1:]...)
		k.putAsList(key, list)
		return handler.NewIntReply(list.Len())
	}

	for i := 1; i < len(args); i++ {
		list.RPush(args[i])
	}

	k.persister.PersistCmd(cmd.Cmd()) // 持久化
	return handler.NewIntReply(list.Len())
}

func (k *KVStore) RPop(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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

	k.persister.PersistCmd(cmd.Cmd()) // 持久化
	if len(poped) == 1 {
		return handler.NewBulkReply(poped[0])
	}

	return handler.NewMultiBulkReply(poped)
}

func (k *KVStore) LRange(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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
func (k *KVStore) SAdd(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if set == nil {
		set = newSetEntity(key)
		k.putAsSet(key, set)
	}

	var added int64
	for _, arg := range args[1:] {
		added += set.Add(string(arg))
	}

	k.persister.PersistCmd(cmd.Cmd()) // 持久化
	return handler.NewIntReply(added)
}

func (k *KVStore) SIsMember(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	if len(args) != 2 {
		return handler.NewSyntaxErrReply()
	}

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

func (k *KVStore) SRem(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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
		k.persister.PersistCmd(cmd.Cmd()) // 持久化
	}
	return handler.NewIntReply(remed)
}

// hash
func (k *KVStore) HSet(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	if len(args)&1 != 1 {
		return handler.NewSyntaxErrReply()
	}

	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if hmap == nil {
		hmap = newHashMapEntity(key)
		k.putAsHashMap(key, hmap)
	}

	for i := 0; i < len(args)-1; i += 2 {
		hkey := string(args[i+1])
		hvalue := args[i+2]
		hmap.Put(hkey, hvalue)
	}

	k.persister.PersistCmd(cmd.Cmd()) // 持久化
	return handler.NewIntReply(int64((len(args) - 1) >> 1))
}

func (k *KVStore) HGet(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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

func (k *KVStore) HDel(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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
		k.persister.PersistCmd(cmd.Cmd()) // 持久化
	}
	return handler.NewIntReply(remed)
}

// sorted set
func (k *KVStore) ZAdd(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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
		zset = newSkiplist(key)
		k.putAsSortedSet(key, zset)
	}

	for i := 0; i < len(scores); i++ {
		zset.Add(scores[i], members[i])
	}

	k.persister.PersistCmd(cmd.Cmd()) // 持久化
	return handler.NewIntReply(int64(len(scores)))
}

func (k *KVStore) ZRangeByScore(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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

func (k *KVStore) ZRem(cmd *database.Command) handler.Reply {
	args := cmd.Args()
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
		k.persister.PersistCmd(cmd.Cmd()) // 持久化
	}
	return handler.NewIntReply(remed)
}
