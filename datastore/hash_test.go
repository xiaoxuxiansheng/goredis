package datastore

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/xiaoxuxiansheng/goredis/database"
	"github.com/xiaoxuxiansheng/goredis/lib"
)

func Test_hashmap_crud(t *testing.T) {
	hashmap := newHashMapEntity("")
	mp := make(map[int]int, 1000)

	rander := rand.New(rand.NewSource(lib.TimeNow().UnixNano()))
	for i := 0; i < 1000; i++ {
		k := rander.Intn(1000)
		v := rander.Intn(1000)
		hashmap.Put(cast.ToString(k), []byte(cast.ToString(v)))
		mp[k] = v
	}

	t.Run("delete", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			k := rander.Intn(1000)
			_, ok := mp[k]
			exist := hashmap.Del(cast.ToString(k))
			assert.Equal(t, ok, exist == 1)
			delete(mp, k)
		}
	})

	t.Run("get", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			k := rander.Intn(1000)
			value := hashmap.Get(cast.ToString(k))
			v, ok := mp[k]
			assert.Equal(t, ok, value != nil)
			if ok {
				assert.Equal(t, v, cast.ToInt(string(value)))
			}
		}
	})
}

func Test_hashmap_to_cmd(t *testing.T) {
	hashmap := newHashMapEntity("")
	rander := rand.New(rand.NewSource(lib.TimeNow().UnixNano()))
	mp := make(map[int]int, 1000)
	// 插入1000条数据
	for i := 0; i < 1000; i++ {
		k := rander.Intn(1000)
		v := rander.Intn(1000)
		hashmap.Put(cast.ToString(k), []byte(cast.ToString(v)))
		mp[k] = v
	}

	cmd := hashmap.ToCmd()
	t.Run("length", func(t *testing.T) {
		assert.Equal(t, 2*len(mp)+2, len(cmd))
	})
	t.Run("command", func(t *testing.T) {
		assert.Equal(t, database.CmdTypeHSet, database.CmdType(cmd[0]))
	})
	t.Run("key", func(t *testing.T) {
		assert.Equal(t, "", string(cmd[1]))
	})

	type kv struct {
		k, v int
	}
	actual := make([]kv, 0, 1000)
	for i := 2; i < len(cmd); i += 2 {
		actual = append(actual, kv{
			k: cast.ToInt(string(cmd[i])),
			v: cast.ToInt(string(cmd[i+1])),
		})
	}

	sort.Slice(actual, func(i, j int) bool {
		if actual[i].k == actual[j].k {
			return actual[i].v < actual[j].v
		}
		return actual[i].k < actual[j].k
	})

	expect := make([]kv, 0, 2*len(mp))
	for k, v := range mp {
		expect = append(expect, kv{
			k: k,
			v: v,
		})
	}
	sort.Slice(expect, func(i, j int) bool {
		if expect[i].k == expect[j].k {
			return expect[i].v < expect[j].v
		}
		return expect[i].k < expect[j].k
	})

	t.Run("member", func(t *testing.T) {
		assert.Equal(t, expect, actual)
	})
}
