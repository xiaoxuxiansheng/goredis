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

func Test_set_crud(t *testing.T) {
	set := newSetEntity("")
	s := make(map[int]struct{}, 1000)
	rander := rand.New(rand.NewSource(lib.TimeNow().UnixNano()))

	t.Run("add", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			member := rander.Intn(1000)
			_, ok := s[member]
			success := set.Add(cast.ToString(member))
			s[member] = struct{}{}
			assert.Equal(t, ok, success == 0)
		}
	})

	t.Run("rem", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			member := rander.Intn(1000)
			_, ok := s[member]
			exist := set.Rem(cast.ToString(member))
			delete(s, member)
			assert.Equal(t, ok, exist == 1)
		}
	})

	t.Run("exist", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			member := rander.Intn(1000)
			_, ok := s[member]
			exist := set.Exist(cast.ToString(member))
			assert.Equal(t, ok, exist == 1)
		}
	})
}

func Test_set_to_cmd(t *testing.T) {
	set := newSetEntity("")
	rander := rand.New(rand.NewSource(lib.TimeNow().UnixNano()))
	s := make(map[int]struct{}, 1000)
	// 插入1000条数据
	for i := 0; i < 1000; i++ {
		member := rander.Intn(1000)
		set.Add(cast.ToString(member))
		s[member] = struct{}{}
	}

	cmd := set.ToCmd()
	t.Run("length", func(t *testing.T) {
		assert.Equal(t, len(s)+2, len(cmd))
	})
	t.Run("command", func(t *testing.T) {
		assert.Equal(t, database.CmdTypeSAdd, database.CmdType(cmd[0]))
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

	expect := make([]int, 0, len(s))
	for member := range s {
		expect = append(expect, member)
	}
	sort.Slice(expect, func(i, j int) bool {
		return expect[i] < expect[j]
	})

	t.Run("member", func(t *testing.T) {
		assert.Equal(t, expect, actual)
	})
}
