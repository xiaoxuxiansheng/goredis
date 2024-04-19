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

func Test_list_crud(t *testing.T) {
	list := newListEntity("")
	l := make([][]byte, 0, 1000)
	rander := rand.New(rand.NewSource(lib.TimeNow().UnixNano()))
	for i := 0; i < 1000; i++ {
		member1 := rander.Intn(1000)
		member2 := rander.Intn(1000)
		list.LPush([]byte(cast.ToString(member1)))
		list.RPush([]byte(cast.ToString(member2)))
		l = append([][]byte{[]byte(cast.ToString(member1))}, l...)
		l = append(l, []byte(cast.ToString(member2)))
	}

	t.Run("range", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			start := rander.Intn(1001)
			end := start + rander.Intn(1000)
			actual := list.Range(int64(start), int64(end))
			expect := l[start : end+1]
			assert.Equal(t, expect, actual)
		}
	})

	t.Run("pop", func(t *testing.T) {
		for i := 0; i < 500; i++ {
			actual := list.LPop(2)
			expect := l[:2]
			l = l[2:]
			assert.Equal(t, expect, actual)

			actual = list.RPop(2)
			expect = l[len(l)-2:]
			l = l[:len(l)-2]
			assert.Equal(t, expect, actual)
		}
	})
}

func Test_list_to_cmds(t *testing.T) {
	list := newListEntity("")
	rander := rand.New(rand.NewSource(lib.TimeNow().UnixNano()))
	l := make([]int, 0, 1000)
	// 插入1000条数据
	for i := 0; i < 1000; i++ {
		member := rander.Intn(1000)
		list.LPush([]byte(cast.ToString(member)))
		l = append(l, member)
	}

	cmd := list.ToCmd()
	t.Run("length", func(t *testing.T) {
		assert.Equal(t, len(l)+2, len(cmd))
	})
	t.Run("command", func(t *testing.T) {
		assert.Equal(t, database.CmdTypeRPush, database.CmdType(cmd[0]))
	})
	t.Run("key", func(t *testing.T) {
		assert.Equal(t, "", string(cmd[1]))
	})

	actual := make([]int, 0, len(cmd)-2)
	for i := 2; i < len(cmd); i++ {
		actual = append(actual, cast.ToInt(string(cmd[i])))
	}
	sort.Slice(actual, func(i, j int) bool {
		return actual[i] < actual[j]
	})

	expect := l
	sort.Slice(expect, func(i, j int) bool {
		return expect[i] < expect[j]
	})

	t.Run("member", func(t *testing.T) {
		assert.Equal(t, expect, actual)
	})
}
